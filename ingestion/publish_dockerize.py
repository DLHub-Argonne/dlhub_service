import boto3
import json
import subprocess
import os
import mdf_toolbox

import psycopg2
import psycopg2.extras

client = boto3.client('stepfunctions')


def dockerize(task, client):
    """
    Use the singularity container to preprocess the data 
    """
    print(task)

    location = task['dlhub']['build_location']
    uuid = task['dlhub']['id']

    os.chdir(location)
    # Start the process
    # 1. build the container
    print("Building container")
    cmd = ['docker', 'build', '-t', uuid, '.']
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    out, err = process.communicate()
    if err is not None:
        # return an error so it will retry this
        print(err)
        raise Exception("Failed to build docker")
    print(out)

    print("Checking if repository exists")
    ecr_arn = None
    ecr_uri = None
    ecr_client = boto3.client('ecr')

    try:
        response = ecr_client.describe_repositories(
            repositoryNames=[uuid]
        )
        ecr_arn = response['repositories'][0]['repositoryArn']
        ecr_uri = response['repositories'][0]['repositoryUri']
    except:
        print ("Creating ECS registry")
        response = ecr_client.create_repository(repositoryName=uuid)
        ecr_arn = response['repository']['repositoryArn']
        ecr_uri = response['repository']['repositoryUri']
    print ("Got ECR repo: %s" % ecr_uri)

    # # 3. Add a tag to the docker container
    print ("Tagging container")
    cmd = ['docker', 'tag', "%s:latest" % uuid, '%s:latest' % ecr_uri]
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    out, err = process.communicate()

    # 4. Login to ECR via docker
    cmd = ['aws', 'ecr', 'get-login', '--no-include-email']
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    out, err = process.communicate()
    login_str = out.decode('utf-8').strip().split(" ")
    process = subprocess.Popen(login_str, stdout=subprocess.PIPE)
    out, err = process.communicate()

    # 5. Push the container to ECR
    print ("Pushing to ECR")
    cmd = ['docker', 'push', ecr_uri]
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    out, err = process.communicate()

    task['dlhub']['ecr_uri'] = ecr_uri
    task['dlhub']['ecr_arn'] = ecr_arn

    return task

def convert_dict(data, conversion_function=str):
    if type(data) is dict:
        string_dict = {}
        for k,v in data.items():
            if type(v) is dict:
                string_dict[k] = convert_dict(v, conversion_function)
            elif type(v) is list:
                string_dict[k] = [convert_dict(item, conversion_function) for item in data[k]]
            else:
                string_dict[k] = conversion_function(v)
        return string_dict
    elif type(data) is list:
        return [convert_dict(item, conversion_function) for item in data]
    else:
        return conversion_function(data)

def search_ingest(task):
    """
    Ingest the servable data into a Globus Search index.

    Args:
        task (dict): the task description.
    """
    print ("Ingesting servable into Search.")

    idx = "dlhub"
    iden = "https://dlhub.org/servables/{}".format(task['dlhub']['id'])
    index = mdf_toolbox.translate_index(idx)

    ingestable = task
    d = [convert_dict(ingestable, str)]

    glist = []

    for document in d:
        gmeta_entry = mdf_toolbox.format_gmeta(document, ["public"], iden)
        glist.append(gmeta_entry)
    gingest = mdf_toolbox.format_gmeta(glist)

    ingest_client = mdf_toolbox.login(services=["search_ingest"])["search_ingest"]
    ingest_client.ingest(idx, gingest)
    print("Ingestion of {} to DLHub servables complete".format(iden))

    #ingestable = mdf_toolbox.format_gmeta(ingestable, acl="public", identifier=iden)
    #ingestable = mdf_toolbox.format_gmeta([ingestable]) # Make it a GIngest list of GMetaEntry

    #ingest_client = mdf_toolbox.login(services=["search_ingest"])["search_ingest"]
    #ingest_client.ingest(index, ingestable)
    #print("Ingestion of {} to DLHub servables complete".format(name))


def monitor():
    """
    Pull jobs from the step function as the preprocess activity
    """
    while True:
        try:
            response = client.get_activity_task(
                activityArn='arn:aws:states:us-east-1:039706667969:activity:dlhub-publish-dockerize',
                workerName='dockerize-activity'
            )

            print (response)
            if response['taskToken']:
                data = response['input']
                try:
                    data = json.loads(data)
                    out = dockerize(data, client)
                    try:
                        ingest_output = search_ingest(out)
                    except Exception as e:
                        print("Failed to ingest to search. {}".format(e))
                    print ("Reporting success")
                    print (out)
                    client.send_task_success(taskToken=response['taskToken'], output=json.dumps(out))
                except Exception as e:
                    print ("Reporting failure")
                    client.send_task_failure(taskToken=response['taskToken'], error='FAILED', cause=str(e))
            else:
                print (".")
        except Exception as e:
            print (e)


if __name__ == "__main__" :
    # test()
    monitor()

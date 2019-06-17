import boto3
import json
import subprocess
import os
import mdf_toolbox
import logging
import psycopg2
import psycopg2.extras

client = boto3.client('stepfunctions')

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.DEBUG, filename='publish_dockerize.log')


def dockerize(task, client):
    """
    Use the singularity container to preprocess the data 
    """

    location = task['dlhub']['build_location']
    uuid = task['dlhub']['id']

    os.chdir(location)
    # Start the process
    # 1. build the container
    logging.debug("Building container")
    cmd = ['docker', 'build', '-t', uuid, '.']
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    out, err = process.communicate()
    if err is not None:
        # return an error so it will retry this
        logging.error(err)
        raise Exception("Failed to build docker")
    logging.debug(out)

    logging.debug("Checking if repository exists")
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
        logging.debug("Creating ECS registry")
        response = ecr_client.create_repository(repositoryName=uuid)
        ecr_arn = response['repository']['repositoryArn']
        ecr_uri = response['repository']['repositoryUri']
    logging.info("Got ECR repo: %s" % ecr_uri)

    # # 3. Add a tag to the docker container
    logging.debug("Tagging container")
    cmd = ['docker', 'tag', "%s:latest" % uuid, '%s:latest' % ecr_uri]
    logging.debug(cmd)

    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    out, err = process.communicate()
    if err:
        logging.error(err)
        raise Exception

    # 4. Login to ECR via docker
    cmd = ['aws', 'ecr', 'get-login', '--no-include-email']
    logging.debug(cmd)
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    out, err = process.communicate()
    login_str = out.decode('utf-8').strip().split(" ")
    process = subprocess.Popen(login_str, stdout=subprocess.PIPE)
    out, err = process.communicate()
    if err:
        logging.error(err)
        raise Exception
    
    # 5. Push the container to ECR
    logging.debug("Pushing to ECR")
    cmd = ['docker', 'push', ecr_uri]
    logging.debug(cmd)
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
    logging.debug("Ingesting servable into Search.")

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
    logging.info("Ingestion of {} to DLHub servables complete".format(iden))


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

            if response['taskToken']:
                data = response['input']
                try:
                    data = json.loads(data)
                    out = dockerize(data, client)
                    try:
                        ingest_output = search_ingest(out)
                    except Exception as e:
                        logging.debug("Failed to ingest to search. {}".format(e))
                    logging.debug("Reporting success")
                    logging.debug(out)
                    client.send_task_success(taskToken=response['taskToken'], output=json.dumps(out))
                except Exception as e:
                    logging.error("Reporting failure")
                    client.send_task_failure(taskToken=response['taskToken'], error='FAILED', cause=str(e))
            else:
                logging.debug(".")
        except Exception as e:
            logging.error(e)


if __name__ == "__main__" :
    monitor()

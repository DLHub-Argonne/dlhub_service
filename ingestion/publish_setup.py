import os
import zipfile
import boto3
import botocore
import json
import uuid
import shutil
from string import Template

"""
This activity worker is responsible for downloading a model and preparing it
to be dockerized. It will need to process the input file to download the servable
and build it.

"""

client = boto3.client('stepfunctions')


def stage_files(location, working_dir):
    """
    Put the files in the working directory.
    """
    print("Staging data")
    if 's3://' in location:
        download_s3_data(location, working_dir)
    elif '/mnt/tmp' in location:
        os.mkdir(working_dir)
        os.rename(location, "{0}/{1}".format(working_dir, location.replace("/mnt/tmp/", '')))

    print("Extracting data")
    # Extract any zip files
    cwd = os.getcwd()    
    os.chdir(working_dir)
    try:
        for item in os.listdir(working_dir):
            if item.endswith('.zip'):
                file_name = os.path.abspath(item)
                zip_ref = zipfile.ZipFile(file_name)
                zip_ref.extractall(working_dir)
                zip_ref.close()
                os.remove(file_name)
    except Exception as e:
        print("Error extracting files. {}".format(e))
    finally:
        os.chdir(cwd)

def download_s3_data(location, working_dir):
    """
    Download the S3 model to a temporary local directory.

    :return: string path to the model
    """

    bucket = location.split("//")[1].split("/")[0]
    key = location.split(bucket)[1][1:]
    print(bucket)
    print(key)

    s3 = boto3.client('s3')
    try:
        # s3.Bucket(bucket).download_file(key, working_dir)
        response = s3.list_objects(
            Bucket=bucket,
            Prefix=key
        )

        # Loop through each file
        for file in response['Contents']:
            # Get the file name and directory, then make sure it exists
            name = "/".join(file['Key'].rsplit('/')[2:])
            directory = working_dir + "/" +  "/".join(name.split("/")[:-1])
            if not os.path.exists(directory):
                os.makedirs(directory)
            # Download each file into this directory
            s3.download_file(bucket, file['Key'], working_dir + '/' + name)

    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
            raise
        else:
            print(e)
            raise
    except Exception as e:
        print(e)

    return working_dir


def setup_ingestion(task, client):
    """
    Download and configure the servable locally. This
    should prepare the directory to be dockerized and published
    to the DLHub service.

    1. Verify the input parameters
    2. Download the servable directory
    3. Configure the directory with necessary components
    """
    print("Starting publication process")

    print("Getting data location")
    model_location = None
    if 'S3' in task['dlhub']['transfer_method']:
        model_location = task['dlhub']['transfer_method']['S3']
    elif 'POST' in task['dlhub']['transfer_method']:
        model_location = task['dlhub']['transfer_method']['path']

    working_uuid = str(uuid.uuid4())
    task['dlhub']['id'] = working_uuid
    # servable_uuid = task['dlhub']['id']
    servable_uuid = working_uuid
    base_working_dir = '/mnt/dlhub_ingest/'
    working_dir = "%s/%s" % (base_working_dir, working_uuid)

    working_dir = working_dir.replace("//", "/")

    # Update the task
    task['dlhub']['build_location'] = working_dir

    # Download the model
    print("Downloading model directory")
    stage_files(model_location, working_dir)

    #config_path = "{}/config".format(directory)
    config_path = "{}/dlhub.json".format(working_dir)
    with open(config_path, 'w') as f:
        f.write(json.dumps(task))

    # Create the requirements file
    create_requirements_file(task, working_dir)

    # Create the apps file
    template_params = {'function': servable_uuid.replace("-", "_"),
                       'executor': servable_uuid}

    with open('../templates/apps.py') as apps_file:
        shim_template = Template(apps_file.read())
        shim_content = shim_template.substitute(template_params)
        with open("{}/apps.py".format(working_dir), 'w') as new_shim:
            new_shim.write(shim_content)

    # Create the docker file
    create_docker_file(task, working_dir)

    print("Done.")
    return task


def create_requirements_file(task, working_dir):
    """
    Create a requirements file to be pip installed. Iterate through
    the dependencies and add them. Also include parsl, toolbox, home_run, etc.

    :param task:
    :param working_dir:
    :return:
    """

    # Get the list of requirements from the schema
    dependencies = []
    try:
        for k, v in task['servable']['dependencies']['python'].items():
            dependencies.append("{0}=={1}".format(k, v))
    except:
        # There are no python dependencies
        pass

    # Add in parsl specific ones
    dependencies.append("parsl==0.6.1")
#    dependencies.append("tensorflow")
    dependencies.append("git+git://github.com/DLHub-Argonne/dlhub_sdk.git")
    dependencies.append("git+git://github.com/DLHub-Argonne/home_run.git")

    with open("{}/requirements.txt".format(working_dir), "a") as fp:
        for dep in dependencies:
            fp.write(dep + "\n")


def create_docker_file(task, working_dir):
    """
    Create a docker file. For now just use a template.

    :param task:
    :param working_dir:
    :return: JSON metadata
    """
    # Put in a Dockerfile
    print("Creating dockerfile")
    docker_file = "../templates/Dockerfile"
    with open(docker_file) as docker:
        # Probably use a template
        docker_content = docker.read()
        with open("%s/Dockerfile" % (working_dir), 'w') as new_docker:
            new_docker.write(docker_content)


def monitor():
    """
    Pull jobs from the step function as the preprocess activity
    """
    while True:
        try:
            response = client.get_activity_task(
                activityArn='arn:aws:states:us-east-1:039706667969:activity:dlhub-publish-setup-model',
                workerName='setup-activity'
            )

            print(response)
            if response['taskToken']:
                data = response['input']
                try:
                    data = json.loads(data)
                    print(data)
                    out = setup_ingestion(data, client)
                    print("Reporting success")
                    print(out)
                    client.send_task_success(taskToken=response['taskToken'], output=json.dumps(out))
                except Exception as e:
                    print("Reporting failure")
                    print(e)
                    client.send_task_failure(taskToken=response['taskToken'], error='FAILED', cause=str(e))
            else:
                print(".")
        except Exception as e:
            print(e)


if __name__ == "__main__" :
    monitor()

import os
import sys
import json
import uuid
import time
import boto3
import base64
import logging
import zipfile
import subprocess

from github import Github
from string import Template

client = boto3.client('stepfunctions')

BASE_WORKING_DIR = '/mnt/dlhub_ingest/'
IMAGE_HOME = '/home/ubuntu/'

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.DEBUG, filename='publish_setup.log')

def _get_dlhub_file(repository):
    """
    Use the github rest api to ensure the dlhub.json file exists.

    :param repository:
    :return:
    """

    token = '83a6cb9912fcd3f7e2b03997b9893fc22e1cd4bd'

    repo = repository.replace("https://github.com/", "")
    repo = repo.replace(".git", "")

    try:
        g = Github(token)
        r = g.get_repo(repo)
        contents = r.get_contents("dlhub.json")
        decoded = base64.b64decode(contents.content)
        return json.loads(decoded)
    except:
        return None


def stage_files(location, working_dir):
    """
    Put the files in the working directory.
    """
    logging.debug("Staging data")
    if 's3://' in location:
        download_s3_data(location, working_dir)
    elif '/mnt/tmp' in location:
        os.mkdir(working_dir)
        os.rename(location, "{0}/{1}".format(working_dir, location.replace("/mnt/tmp/", '')))

    logging.debug(f"Extracting data: {working_dir}")
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
        logging.error("Error extracting files. {}".format(e))
    finally:
        os.chdir(cwd)


def download_s3_data(location, working_dir):
    """
    Download the S3 model to a temporary local directory.

    :return: string path to the model
    """

    bucket = location.split("//")[1].split("/")[0]
    key = location.split(bucket)[1][1:]

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
            logging.error("The object does not exist.")
            raise
        else:
            logging.error(e)
            raise
    except Exception as e:
       logging.error(e)

    return working_dir


def _configure_build_env(servable_uuid, working_dir, working_image, dlhub_json_file):
    """
    Create a directory to build the thing

    :param task:
    :return: task
    """

    if not os.path.exists(working_dir):
        os.makedirs(working_dir)

    logging.debug("Creating dockerfile")
    docker_file_contents = """from {0}

ADD . {1}

RUN pip install parsl==0.6.1
RUN pip install dlhub_sdk
RUN pip install git+git://github.com/DLHub-Argonne/home_run.git
""".format(working_image, IMAGE_HOME)

    with open("%s/Dockerfile" % (working_dir), 'w') as new_docker:
        new_docker.write(docker_file_contents)

    template_params = {'function': servable_uuid.replace("-", "_"),
                       'executor': servable_uuid}

    with open('templates/apps.py') as apps_file:
        shim_template = Template(apps_file.read())
        shim_content = shim_template.substitute(template_params)
        with open("{}/apps.py".format(working_dir), 'w') as new_shim:
            new_shim.write(shim_content)

    with open("%s/dlhub.json" % (working_dir), 'w') as dlhub_file:
        dlhub_file.write(json.dumps(dlhub_json_file))


def ingest(task, client):
    """
    Ingest the data

    :param data:
    :param client:
    :return:
    """

    logging.debug("Starting ingest")
    if 'dlhub' not in task:
        task['dlhub'] = {}

    model_location = None
    if 'S3' in task['dlhub']['transfer_method']:
        model_location = task['dlhub']['transfer_method']['S3']
    elif 'POST' in task['dlhub']['transfer_method']:
        model_location = task['dlhub']['transfer_method']['path']
    if 'repository' in task:
        model_location = task['respoitory']

    logging.info(task)

    servable_uuid = str(uuid.uuid4())
    try:
        task['dlhub']['id'] = servable_uuid
        task['dlhub']['user_id'] = task['user_id']
        task['dlhub']['shorthand_name'] = task['shorthand_name']
    except Exception as e:
        logging.error(f'key moved: {e}')
        logging.debug('continuing')
    logging.debug(task)

    working_name = "{0}-{1}".format(servable_uuid, str(time.time()).split(".")[0])
    working_dir = ("%s/%s" % (BASE_WORKING_DIR, working_name)).replace("//", "/")
    working_image = "{0}-img".format(working_name)

    try:
        stage_files(model_location, working_dir)
    except Exception as e:
        logging.error("Error staging data: ", e)


    tmp_image = "{0}-tmp".format(working_image)

    logging.debug('running repo2docker')
    # Use repo2docker to build the container
    cmd = "jupyter-repo2docker --no-run --image-name {0} {1}".format(tmp_image,
                                                                     working_dir)
    logging.debug("Repo2docker: {}".format(cmd))
    subprocess.call(cmd.split(" "))
    
    logging.debug("Configuring working dir: {}".format(working_dir))
    _configure_build_env(servable_uuid, working_dir, tmp_image, task)
    
    logging.debug('Running repo2docker the second time')
    cmd = "jupyter-repo2docker --no-run --image-name {0} {1}".format(working_image,
                                                                     working_dir)
    subprocess.call(cmd.split(" "))

    task['dlhub']['build_location'] = working_dir

    return task


def monitor():
    """
    Pull jobs from the step function as the preprocess activity
    """
    # ingest({'repository': 'https://github.com/ryanchard/test_repo2docker.git'}, '')
    # return

    while True:
        try:
            response = client.get_activity_task(
                activityArn='arn:aws:states:us-east-1:039706667969:activity:dlhub-publish-setup-model',
                workerName='setup-activity'
            )

            if response['taskToken']:
                data = response['input']
                try:
                    data = json.loads(data)
                    logging.debug(data)
                    out = ingest(data, client)
                    logging.info("Reporting success")
                    logging.info(out)
                    client.send_task_success(taskToken=response['taskToken'], output=json.dumps(out))
                except Exception as e:
                    logging.error("Reporting failure")
                    logging.error(e)
                    client.send_task_failure(taskToken=response['taskToken'], error='FAILED', cause=str(e))
            else:
                logging.info(".")
        except Exception as e:
            logging.error(e)


if __name__ == "__main__" :
    monitor()

import os
import sys
import json
import uuid
import time
import boto3
import base64
import subprocess

from github import Github
from string import Template

client = boto3.client('stepfunctions')

BASE_WORKING_DIR = '/mnt/dlhub_ingest/'
IMAGE_HOME = '/home/ubuntu/'

# BASE_WORKING_DIR = '/home/ryan/src/DLHub/test/'
# IMAGE_HOME = '/home/ryan/'

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


def _configure_build_env(servable_uuid, working_dir, working_image):
    """
    Create a directory to build the thing

    :param task:
    :return: task
    """

    if not os.path.exists(working_dir):
        os.makedirs(working_dir)

    print("Creating dockerfile")
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

    with open('../templates/apps.py') as apps_file:
        shim_template = Template(apps_file.read())
        shim_content = shim_template.substitute(template_params)
        with open("{}/apps.py".format(working_dir), 'w') as new_shim:
            new_shim.write(shim_content)


def ingest(task, client):
    """
    Ingest the data

    :param data:
    :param client:
    :return:
    """

    print("Starting ingest")
    if 'dlhub' not in task:
        task['dlhub'] = {}

    repo = task['repository']

    #dlhub_contents = _get_dlhub_file(repo)
    dlhub_contents = task
    print('here with current task')
    print(dlhub_contents)
    if not dlhub_contents:
        return {"Error": "Could not read dlhub.json file in repository."}
    else:
        task.update(dlhub_contents)
    
    print(task)

    servable_uuid = str(uuid.uuid4())

    task['dlhub']['id'] = servable_uuid
    task['dlhub']['user_id'] = task['user_id']
    task['dlhub']['shorthand_name'] = task['shorthand_name']

    working_name = "{0}-{1}".format(servable_uuid, str(time.time()).split(".")[0])
    working_dir = ("%s/%s" % (BASE_WORKING_DIR, working_name)).replace("//", "/")
    working_image = "{0}-img".format(working_name)

    print("Configuring working dir: {}".format(working_dir))

    _configure_build_env(servable_uuid, working_dir, working_image)

    print('running repo2docker')
    # Use repo2docker to build the container
    cmd = "jupyter-repo2docker --no-run --image-name {0} {1}".format(working_image,
                                                                     repo)
    print("Repo2docker: {}".format(cmd))
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
                activityArn='arn:aws:states:us-east-1:039706667969:activity:dlhub-publish-repo2docker',
                workerName='setup-activity'
            )

            print (response)
            if response['taskToken']:
                data = response['input']
                try:
                    data = json.loads(data)
                    print (data)
                    out = ingest(data, client)
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
    monitor()

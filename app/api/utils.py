import base64
import boto3
import uuid
import json

from config import _load_dlhub_client, GIT_TOKEN
from flask import request
from github import Github


def create_presigned_post(bucket_name, object_name,
                          fields=None, conditions=None, expiration=3600):
    """Generate a presigned URL S3 POST request to upload a file

    :param bucket_name: string
    :param object_name: string
    :param fields: Dictionary of prefilled form fields
    :param conditions: List of conditions to include in the policy
    :param expiration: Time in seconds for the presigned URL to remain valid
    :return: Dictionary with the following keys:
        url: URL to post to
        fields: Dictionary of form fields and values to submit with the POST
    :return: None if error.
    """

    # Generate a presigned S3 POST URL
    s3_client = boto3.client('s3')
    try:
        response = s3_client.generate_presigned_post(bucket_name,
                                                     object_name,
                                                     Fields=fields,
                                                     Conditions=conditions,
                                                     ExpiresIn=expiration)
    except Exception as e:
        print(e)
        return None

    # The response contains the presigned URL and required fields
    return response


def _start_flow(cur, conn, flow_arn, input_data):
    """
    Start an AWS SFN flow.

    :return:
    """
    sfn_client = boto3.client('stepfunctions')
    task_uuid = str(uuid.uuid4())
    response = sfn_client.start_execution(
        stateMachineArn=flow_arn,
        name=str(uuid.uuid4()),
        input=json.dumps(input_data)
    )
    res = _create_task(cur, conn, input_data, response, task_uuid)
    return res


def _get_dlhub_file_from_github(repository):
    """
    Use the github rest api to ensure the app.json file exists.

    :param repository:
    :return:
    """

    if 'github.com' not in repository:
        return None

    repo = repository.replace("https://github.com/", "")
    repo = repo.replace(".git", "")

    try:
        g = Github(GIT_TOKEN)
        r = g.get_repo(repo)
        contents = r.get_contents("dlhub.json")
        decoded = base64.b64decode(contents.content)

        return json.loads(decoded)
    except Exception as e:
        print(e)
        return None


############
# Database #
############
def _create_task(cur, conn, input_data, response, task_uuid, task_type='ingest', result=''):
    """
    Insert a task into the database.

    :param input_data:
    :param response:
    :param task_uuid:
    :return:
    """
    try:
        arn_val = ''
        if response:
            if 'executionArn' in response:
                arn_val = response['executionArn']
        if not input_data:
            input_data = []
        query = "INSERT INTO tasks (uuid, type, input, arn, status, result) values (%s, %s, %s, %s, 'RUNNING', %s)"
        cur.execute(query, (task_uuid, task_type, json.dumps(input_data), arn_val, result))
        conn.commit()
    except Exception as e:
        print(e)
    res = {"status": "RUNNING", "task_id": task_uuid}
    return res


def _introspect_token(headers):
    """
    Decode the token and retrieve the user's details

    :param headers:
    :return:
    """
    user_name = None
    user_id = None
    if 'Authorization' in headers:
        token = request.headers.get('Authorization')

        token = token.split(" ")[1]
        try:
            client = _load_dlhub_client()
            auth_detail = client.oauth2_token_introspect(token)

            user_name = auth_detail['username']
            user_id = auth_detail['sub']
        except Exception as e:
            print('Auth error:', e)
    return user_name, user_id


def _resolve_namespace_model(cur, conn, namespace, model_name):
    """
    Return the uuid of the most recent model with this namespace.
    """
    servable_uuid = None
    try:
        command = (
            "SELECT * from servables where dlhub_name = '{}/{}' order by id desc limit 1".format(namespace, model_name))
        print(command)
        cur.execute(command)
        rows = cur.fetchall()
        if len(rows) > 0:
            for r in rows:
                servable_uuid = r['uuid']
    except Exception as e:
        print(e)
    return servable_uuid


def _get_user(cur, conn, headers):
    """
    Get the user details from the database.

    :param headers:
    :return:
    """

    user_name, user_id = _introspect_token(headers)
    short_name = None
    user_id = None

    print('Authing user: {}'.format(user_name))
    if not user_name:
        return (None, None, None)

    # Now check if it is in the database.
    try:
        cur.execute("SELECT * from users where user_name = '%s'" % user_name)
        rows = cur.fetchall()
        if len(rows) > 0:
            for r in rows:
                short_name = r['namespace']
                user_id = r['id']
        else:
            short_name = "{name}_{org}".format(name=user_name.split(
                "@")[0], org=user_name.split("@")[1].split(".")[0])
            cmd = "INSERT into users (user_name, globus_name, namespace, globus_uuid) values "\
                  "('{name}', '{globus}', '{short}', '{globus_uuid}') RETURNING id".format(
                      name=user_name, globus=user_name, short=short_name, globus_uuid=user_id)
            cur.execute(cmd)
            conn.commit()
            user_id = cur.fetchone()[0]
    except Exception as e:
        print(e)
    return user_id, user_name, short_name

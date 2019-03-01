import numpy as np
import base64
import boto3
import uuid
import json

from config import _load_dlhub_client, GIT_TOKEN
from flask import request, abort
from github import Github


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
        contents = r.get_contents("app.json")
        decoded = base64.b64decode(contents.content)

        return json.loads(decoded)
    except Exception as e:
        print(e)
        return None


############
# Database #
############
def _create_task(cur, conn, input_data, response, task_uuid):
    """
    Insert a task into the database.

    :param input_data:
    :param response:
    :param task_uuid:
    :return:
    """
    try:
        query = "INSERT INTO tasks (uuid, type, input, arn, status) values ('%s', 'ingest', '%s', '%s', 'RUNNING')" % (
            task_uuid, json.dumps(input_data).replace("'", "''"), response['executionArn'])
        cur.execute(query)
        conn.commit()
    except Exception as e:
        print(e)
    res = {"status": "RUNNING", "task_id": task_uuid}
    return res


def _log_invocation(cur, conn, response, request_start, request_end, servable_uuid, user_id, input_data, type):
    """
    Log the invocation time in the database.

    :return:
    """
    try:
        invocation_time = None
        if 'invocation_time' in response:
            invocation_time = response['invocation_time']
        request_time = (request_end - request_start) * 1000
        query = "INSERT INTO invocation_logs (servable, input, invocation, request, function, user_id) values ('{}', '{}', {}, " \
                "{}, '{}', {})".format(
            servable_uuid, len(input_data), invocation_time, request_time, type, user_id)
        cur.execute(query)
        conn.commit()
    except Exception as e:
        print(e)


def _decode_result(tmp_res_lst):
    """
    Try to decode the result to make it jsonifiable.

    :param response_list:
    :return: jsonifiable list
    """

    response_list = []
    if isinstance(tmp_res_lst, list):
        for tmp_res in tmp_res_lst:
            if isinstance(tmp_res, np.ndarray):
                response_list.append(tmp_res.tolist())
            else:
                response_list.append(tmp_res)
    elif isinstance(tmp_res_lst, dict):
        response_list = tmp_res_lst
    elif isinstance(tmp_res_lst, np.ndarray):
        response_list.append(tmp_res_lst.tolist())
    else:
        response_list.append(tmp_res_lst)
    return response_list


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
        command = ("SELECT * from servables where dlhub_name = '{}/{}' order by id desc limit 1".format(namespace, model_name))
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
            short_name = "{name}_{org}".format(name=user_name.split("@")[0], org=user_name.split("@")[1].split(".")[0])
            cmd = "INSERT into users (user_name, globus_name, namespace, globus_uuid) values "\
                  "('{name}', '{globus}', '{short}', '{globus_uuid}') RETURNING id".format(name=user_name, globus=user_name, short=short_name, globus_uuid=user_id)
            cur.execute(cmd)
            conn.commit()
            user_id = cur.fetchone()[0]
    except Exception as e:
        print(e)
    return user_id, user_name, short_name


def _check_user_access(cur, conn, servable_uuid, user_name):
    """
    Determine whether this user has permission to invoke the servable

    :return str: site
    """
    site = None
    try:
        cur.execute("SELECT * from servables where uuid = '%s'" % servable_uuid)
        rows = cur.fetchall()
        for r in rows:
            site = r['servable']
            if r['protected']:
                if not user_name:
                    continue
                query = "SELECT * from servables, users, servable_whitelist where users.globus_name = '%s' and " \
                        "users.id = servable_whitelist.user_id and servables.uuid = '%s' and servables.id = " \
                        "servable_whitelist.servable_id" % (user_name, r['uuid'])
                print(query)
                cur.execute(query)
                prot_rows = cur.fetchall()
                if len(prot_rows) > 0:
                    return site
            else:
                return site
    except Exception as e:
        print(e)
    return None

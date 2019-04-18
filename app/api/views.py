import jsonpickle
import threading
import pickle
import boto3
import json
import uuid
import time
import os

from .utils import (_get_user, _start_flow, _decode_result, _resolve_namespace_model,
                    _check_user_access, _log_invocation, _get_dlhub_file_from_github,
                    _create_task)
from flask import Blueprint, request, abort
from werkzeug import secure_filename
from .zmqserver import ZMQServer



from config import (_get_db_connection, PUBLISH_FLOW_ARN, PUBLISH_REPO_FLOW_ARN)

conn, cur = _get_db_connection()

# Flask
api = Blueprint("api", __name__)

# ZMQ
zmq_server = ZMQServer()


def _perform_invocation(servable_uuid, request, type='test'):
    """
    Perform the invocation.

    :param servable_uuid:
    :param request:
    :param type:
    :return:
    """
    user_id, user_name, short_name = _get_user(cur, conn, request.headers)
    site = _check_user_access(cur, conn, servable_uuid, user_name)

    if not user_name:
        abort(400, description="Error: You must be logged in to perform this function.")
    if not request.json:
        abort(400, description="Error: Requires JSON input.")
    if not site:
        abort(400, description="Permission denied. Cannot access servable {0}".format(servable_uuid))
    input_data = request.json

    exec_flag = 1

    response = None

    try:
        data = []
        if 'data' in input_data:
            data = input_data['data']
        elif 'python' in input_data:
            data = jsonpickle.decode(input_data['python'])

        obj = (exec_flag, site, data)

        # Manage asynchronous request
        async_val = False
        #if 'asynchronous' in input_data:
        #    async_val = input_data['asynchronous']
        async_val = True
        if async_val:
            task_uuid = str(uuid.uuid4())
            req_thread = threading.Thread(target=_async_invocation, args=(obj, task_uuid, servable_uuid, user_id, data, exec_flag))
            req_thread.start()
            response = {"task_id": task_uuid}
            return json.dumps(response)
        else:
            request_start = time.time()

            res = zmq_server.request(pickle.dumps(obj))
            response = pickle.loads(res)
            request_end = time.time()

            _log_invocation(cur, conn, response, request_start, request_end, servable_uuid, user_id, data, exec_flag)

    except Exception as e:
        print("Failed to perform invocation %s" % e)
        return json.dumps({"InternalError": "Failed to perform invocation: %s" % e})

    if not response:
        abort(500, description="Error: Internal service error.")

    try:
        response_list = _decode_result(response['response'])
        
        return json.dumps(response_list)
    except Exception as e:
        print("Failed to return output %s" % e)
        return json.dumps({"InternalError": "Failed to return output: %s" % e})


def _async_invocation(obj, task_uuid, servable_uuid, user_id, data, exec_flag):
    """
    Perform an asynchronous invocation to a servable then update the database once the task completes.
    """

    _create_task(cur, conn, '', '', task_uuid, 'invocation', '')

    request_start = time.time()
    res = zmq_server.request(pickle.dumps(obj))
    response = pickle.loads(res)
    request_end = time.time()

    _log_invocation(cur, conn, response, request_start, request_end, servable_uuid, user_id, data, exec_flag)

    status = 'COMPLETED'

    output = 'Error: Internal Service Error'
    try:
        response_list = _decode_result(response['response'])
        output = json.dumps(response_list)
    except Exception as e:
        output = json.dumps({"InternalError": "Failed to return output: %s" % e})

    query = "UPDATE tasks set status = '%s', result = '%s' where uuid = '%s'" % (status, output, task_uuid)
    cur.execute(query)
    conn.commit()


@api.route("/servables/<servable_namespace>/<servable_name>/run", methods=['POST'])
def api_run_namespace(servable_namespace, servable_name):
    """
    Invoke a servable.

    :param servable_uuid:
    :return:
    """
    servable_uuid = _resolve_namespace_model(cur, conn, servable_namespace, servable_name)
    output = _perform_invocation(servable_uuid, request, type='run')
    return output


@api.route("/servables/<servable_uuid>/run", methods=['POST'])
def api_run(servable_uuid):
    """
    ** DEPRECATED NOW WE USE NAMESPACES **

    Invoke a servable.

    :param servable_uuid:
    :return:
    """
    output = _perform_invocation(servable_uuid, request, type='run')
    return output



########################
# SERVABLE PUBLICATION #
########################
@api.route("/publish", methods=['post'])
def publish_servables():
    """
    Publish a servable.

    :return:
    """
    user_id, user_name, short_name = _get_user(cur, conn, request.headers)
    if not user_name:
        abort(400, description="Error: You must be logged in to perform this function.")
    input_data = None
    if not request.json:
        try:
            posted_file = request.files['file']
            posted_data = json.load(request.files['json'])             
            storage_path = os.path.join("/mnt/tmp", secure_filename(posted_file.filename))
            posted_file.save(storage_path)
            input_data = posted_data
            input_data['app']['transfer_method']['path'] = storage_path
        except Exception as e:
            print('Error: {}'.format(e))
            abort(400, description="No JSON posted. Assumed file, but something went wrong: {}".format(e))
    else:
        input_data = request.json
    if not input_data:
        abort(400, description="Failed to load app.json input data")

    # insert owner and timestamp
    input_data['app']['owner'] = short_name
    input_data['app']['publication_date'] = int(round(time.time() * 1000))
    input_data['app']['user_id'] = user_id

    model_name = input_data['app']['name']
    shorthand_name = "{name}/{model}".format(name=short_name, model=model_name.replace(" ", "_"))

    input_data['app']['shorthand_name'] = shorthand_name

    flow_arn = PUBLISH_FLOW_ARN

    res = _start_flow(cur, conn, flow_arn, input_data)
    res['servable'] = shorthand_name
    return json.dumps(res)


@api.route("/publish_repo", methods=['post'])
def publish_repo_servables():
    """
    Publish a servable via repo2docker.

    :return:
    """
    user_id, user_name, short_name = _get_user(cur, conn, request.headers)
    if not user_name:
        abort(400, description="Error: You must be logged in to perform this function.")
    if not request.json:
        abort(400, description="Error: Requires JSON input.")

    input_data = request.json

    input_data['app']['owner'] = short_name
    input_data['app']['publication_date'] = int(round(time.time() * 1000))
    input_data['user_id'] = user_id

    model_name = _get_dlhub_file_from_github(input_data['repository'])['app']['name']
    shorthand_name = "{name}/{model}".format(name=short_name, model=model_name.replace(" ", "_"))

    input_data['shorthand_name'] = shorthand_name

    flow_arn = PUBLISH_REPO_FLOW_ARN

    res = _start_flow(cur, conn, flow_arn, input_data)
    res['servable'] = shorthand_name
    return json.dumps(res)


###################
# Other endpoints #
###################
@api.route("/<task_uuid>/status", methods=['GET'])
def status(task_uuid):
    """
    Check the status of a task.

    :param task_uuid:
    :return:
    """
    user_id, user_name, short_name = _get_user(cur, conn, request.headers)
    if not user_name:
        abort(400, description="Error: You must be logged in to perform this function.")

    try:
        exec_arn = None
        status = None
        result = ''

        cur.execute("SELECT * from tasks where uuid = '%s'" % task_uuid)
        rows = cur.fetchall()

        for r in rows:
            exec_arn = r['arn']
            status = r['status']
            result = r['result']
        res = {'status': status}

        if exec_arn:
            # Check sfn for status
            sfn_client = boto3.client('stepfunctions')
            response = sfn_client.describe_execution(executionArn=exec_arn)
            status = response['status']
            res['status'] = status
            if 'output' in response:
                output = response['output']
                res['output'] = output
            query = "UPDATE tasks set status = '%s' where uuid = '%s'" % (status, task_uuid)
            cur.execute(query)
            conn.commit()
        # Otherwise, this is an async request
        else:
            res = {'status': status, 'result': result}
        return json.dumps(res, default=str)
    except Exception as e:
        print(e)
        return json.dumps({'InternalError': e})


@api.route("/servables", methods=['GET'])
def api_servables():
    """
    Get a list of all accessible servables.

    :return:
    """
    print('/servables')
    user_id, user_name, short_name = _get_user(cur, conn, request.headers)
    if not user_name:
        print('Aborting.')
        abort(400, description="Error: You must be logged in to perform this function.")

    try:
        query = "SELECT distinct on (dlhub_name) * from servables where status = 'READY' order by dlhub_name, id desc"
        cur.execute(query)
        rows = cur.fetchall()
        res = []
        for r in rows:
            if r['protected']:
                if not user_name:
                    continue
                query = "SELECT * from servables, users, servable_whitelist where users.globus_name = '%s' and " \
                        "users.id = servable_whitelist.user_id and servables.uuid = '%s' and servables.id = " \
                        "servable_whitelist.servable_id" % (user_name, r['uuid'])
                cur.execute(query)
                prot_rows = cur.fetchall()
                if len(prot_rows) > 0:
                    res.append(r)
                continue
            res.append(r)
        return json.dumps(res, default=str)
    except Exception as e:
        print(e)
        return json.dumps({"InternalError": e})


@api.route("/servables/<servable_uuid>/status", methods=['GET'])
def api_servable_status(servable_uuid):
    """
    Check the status of a servable.

    :param servable_uuid:
    :return:
    """
    user_id, user_name, short_name = _get_user(cur, conn, request.headers)
    if not user_name:
        abort(400, description="Error: You must be logged in to perform this function.")

    status = {}
    try:
        cur.execute("SELECT * from servables where uuid = '%s'" % servable_uuid)
        rows = cur.fetchall()
        for r in rows:
            status = {'status': r['status']}
    except Exception as e:
        print(e)
        return json.dumps({"InternalError": e})

    print(status)

    return json.dumps(status)


@api.route("/namespaces", methods=['GET'])
def get_namespaces():
    """
    Return the current namespace of the user

    :return:
    """
    user_id, user_name, short_name = _get_user(cur, conn, request.headers)
    if not user_name:
        abort(400, description="Error: You must be logged in to perform this function.")
    res = {'namespace': short_name}
    return json.dumps(res)


@api.route("/servables/<servable_namespace>/<servable_name>", methods=['DELETE'])
def api_delete_servable(servable_namespace, servable_name):
    """
    Delete a servable

    :param servable_uuid:
    :return:
    """
    user_id, user_name, short_name = _get_user(cur, conn, request.headers)
    if not user_name:
        abort(400, description="Error: You must be logged in to perform this function.")

    print("deleting namespaced servable")
    print(servable_namespace)
    print(servable_name)
    servable_uuid = _resolve_namespace_model(cur, conn, servable_namespace, servable_name)
    print(servable_uuid)
    query = "update servables set status = 'DELETED' where uuid = '{0}'".format(servable_uuid)
    print(query)
    try:
        cur.execute(query)
        conn.commit()
    except Exception as e:
        print(e)
        return json.dumps({"InternalError": e})

    return json.dumps({'status': 'done'})

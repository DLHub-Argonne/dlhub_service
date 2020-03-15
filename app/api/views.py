import jsonpickle
import threading
import pickle
import boto3
import json
import uuid
import time
import os
from config import _load_dlhub_client
from .utils import (_get_user, _start_flow, _decode_result, _resolve_namespace_model,
                    _check_user_access, _log_invocation, _get_dlhub_file_from_github,
                    _create_task)
from flask import Blueprint, request, abort, jsonify
from werkzeug.utils import secure_filename
from .zmqserver import ZMQServer


from config import (_get_db_connection, PUBLISH_FLOW_ARN, PUBLISH_REPO_FLOW_ARN)

conn, cur = _get_db_connection()

# Flask
api = Blueprint("api", __name__)

# ZMQ
zmq_server = ZMQServer()


def _perform_invocation(servable_uuid, request, type='test'):
    """Invoke a servable

    Args:
        servable_uuid (string): UUID of servable to be executed
        request (Request): Request from the user
        type (str): Whether to execute the run operation of the servable
    Returns:
        (str): JSON-formatted results of executing the servable
    """

    user_id, user_name, short_name = _get_user(cur, conn, request.headers)
    site = None
    exec_flag = 1
    if isinstance(servable_uuid, list):
        exec_flag = 2
        site = []
        for s in servable_uuid:
            site.append(check_user_access(cur, conn, s, user_name))
    else:
        site = _check_user_access(cur, conn, servable_uuid, user_name)
        #exec_flag = 2
        #site = [site, site, site]

    print(site)
    # Return errors if the user does not have access to the servable
    if not user_name:
        abort(400, description="Error: You must be logged in to perform this function.")
    if not site:
        abort(400, description="Permission denied. Cannot access servable {0}".format(servable_uuid))

    # Return errors if the user did not provide formatted data
    if not request.json:
        abort(400, description="Error: Requires JSON input.")

    # Get the input data for the function
    input_data = request.json

    # Perform the invocation
    try:
        data = []
        if 'data' in input_data:
            data = input_data['data']
        elif 'python' in input_data:
            # TODO (lw): Is decoding in the web service dangerous? Should we push this to the shim?
            data = jsonpickle.decode(input_data['python'])

        # Send request to service
        obj = (exec_flag, site, data)

        # Manage asynchronous request
        async_val = False
        if 'asynchronous' in input_data:
            async_val = input_data['asynchronous']

        task_uuid = str(uuid.uuid4())
        if async_val:
            req_thread = threading.Thread(target=_async_invocation, args=(obj, task_uuid, servable_uuid, user_id, data, exec_flag))
            req_thread.start()
            response = {"task_id": task_uuid}
            return json.dumps(response), 202
        else:
            request_start = time.time()
            # TODO (lw): Is this also dangerous
            res = zmq_server.request(pickle.dumps(obj))
            response = pickle.loads(res)
            request_end = time.time()

            _log_invocation(cur, conn, response, request_start, request_end, servable_uuid, user_id, data, exec_flag, task_uuid)

    except Exception as e:
        print("Failed to perform invocation %s" % e)
        return json.dumps({"InternalError": "Failed to perform invocation: %s" % e})

    if not response:
        # TODO (lw): This should put some kind of logging information
        abort(500, description="Error: Internal service error.")

    try:
        # Attempt to make the output JSONify-able
        response_list = _decode_result(response['response'])

        # Send to user as a string
        return json.dumps(response_list)
    except Exception as e:
        print("Failed to return output %s" % e)
        # TODO (lw): Should these come back as a non-200 status?
        return json.dumps({"InternalError": "Failed to return output: %s" % e})


def _async_invocation(obj, task_uuid, servable_uuid, user_id, data, exec_flag):
    """
    Perform an asynchronous invocation to a servable then update the database once the task completes.
    """
    return jsonify({"error": "This function is now deprecated. Please update your dlhub_sdk."})
    _create_task(cur, conn, '', '', task_uuid, 'invocation', '')

    request_start = time.time()
    res = zmq_server.request(pickle.dumps(obj))
    response = pickle.loads(res)
    request_end = time.time()

    _log_invocation(cur, conn, response, request_start, request_end, servable_uuid, user_id, data, exec_flag, task_uuid)

    status = 'COMPLETED'

    output = 'Error: Internal Service Error'
    try:
        response_list = _decode_result(response['response'])
        output = json.dumps(response_list)
    except Exception as e:
        output = json.dumps({"InternalError": "Failed to return output: %s" % e})

    query = "UPDATE tasks set status = %s, result = %s where uuid = %s"
    cur.execute(query, (status, output, task_uuid))
    conn.commit()


@api.route("/servables/<servable_namespace>/<servable_name>/run", methods=['POST'])
def api_run_namespace(servable_namespace, servable_name):
    """
    Invoke a servable.

    Args:
        servable_namespace (str): Owner of the servable
        servable_name (str): Name of the servable
    Returns:
        (str): Response from the servable
    """
    return jsonify({"error": "This function is now deprecated. Please update your dlhub_sdk."})
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
    return jsonify({"error": "This function is now deprecated. Please update your dlhub_sdk."})
    output = _perform_invocation(servable_uuid, request, type='run')
    return output


@api.route("/pipelines/run", methods=['POST'])
def api_run_pipeline():
    """
    Invoke a servable.

    Returns:
        (str): Response from the servables
    """
    return jsonify({"error": "This function is now deprecated. Please update your dlhub_sdk."})
    servables = []
    if 'servables' in request.json():
        servables = request.json()['servables']

    resolved_servables = []
    for x in servables:
        servable_namespace = x.split('/')[0]
        servable_name = x.split('/')[1]
        resolved_servables.append(_resolve_namespace_model(cur, conn, servable_namespace, servable_name))
        
    output = _perform_invocation(resolved_servables, request, type='run')
    return output



########################
# SERVABLE PUBLICATION #
########################

@api.route("/publish", methods=['post'])
def publish_servables():
    """Publish a servable via a POST request

    Returns:
        (str): JSON-encoded status information
    """

    # Check the user credentials
    user_id, user_name, short_name = _get_user(cur, conn, request.headers)
    if not user_name:
        abort(400, description="Error: You must be logged in to perform this function.")

    # Get the servable data
    input_data = None
    if not request.json:
        try:
            posted_file = request.files['file']
            posted_data = json.load(request.files['json'])             
            storage_path = os.path.join("/mnt/tmp", secure_filename(posted_file.filename))
            posted_file.save(storage_path)
            input_data = posted_data
            input_data['dlhub']['transfer_method']['path'] = storage_path
        except Exception as e:
            print('Error: {}'.format(e))
            abort(400, description="No JSON posted. Assumed file, but something went wrong: {}".format(e))
    else:
        input_data = request.json
    if not input_data:
        abort(400, description="Failed to load app.json input data")

    # Get a dependent token for funcX
    if 'Authorization' in request.headers:
        token = request.headers.get('Authorization')

        token = token.split(" ")[1]
        try:
            client = _load_dlhub_client()
            auth_detail = client.oauth2_get_dependent_tokens(token)
            fx_auth = auth_detail.by_scopes['https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/all']
            input_data['dlhub']['funcx_token'] = fx_auth['access_token']
        except Exception as e:
            print(e)
    # Insert owner name and time-stamp into metadata
    input_data['dlhub']['owner'] = short_name
    input_data['dlhub']['publication_date'] = int(round(time.time() * 1000))
    input_data['dlhub']['user_id'] = user_id

    # Generate model shortname and store in metadata
    model_name = input_data['dlhub']['name']
    shorthand_name = "{name}/{model}".format(name=short_name, model=model_name.replace(" ", "_"))
    input_data['dlhub']['shorthand_name'] = shorthand_name

    # Start publication flow
    flow_arn = PUBLISH_FLOW_ARN
    res = _start_flow(cur, conn, flow_arn, input_data)
    res['servable'] = shorthand_name

    return json.dumps(res)


@api.route("/publish_repo", methods=['post'])
def publish_repo_servables():
    """Publish a servable via repo2docker

    Returns:
        (str): JSON-formatted status information
    """

    # Check user credentials
    user_id, user_name, short_name = _get_user(cur, conn, request.headers)
    if not user_name:
        abort(400, description="Error: You must be logged in to perform this function.")

    # Verify format of request
    if not request.json:
        abort(400, description="Error: Requires JSON input.")

    # Generate the owner
    input_data = request.json

    # TODO: Duplicated code with above function. Make utility
    input_data['dlhub']['owner'] = short_name
    input_data['dlhub']['publication_date'] = int(round(time.time() * 1000))
    input_data['user_id'] = user_id

    # Make name from repository ID
    model_name = _get_dlhub_file_from_github(input_data['repository'])['dlhub']['name']
    shorthand_name = "{name}/{model}".format(name=short_name, model=model_name.replace(" ", "_"))
    input_data['shorthand_name'] = shorthand_name

    # Start publication flow
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

    Args:
        task_uuid (str): UUID of task
    Returns:
        (str): JSON-encoded status information
    """

    # Get user credentials
    # TODO (lw): Are we concerned about users seeing other users's tasks?
    user_id, user_name, short_name = _get_user(cur, conn, request.headers)
    if not user_name:
        abort(400, description="Error: You must be logged in to perform this function.")

    # Run the check
    try:
        exec_arn = None
        status = None
        result = ''
        invocation_time = None

        # Find the status ID from the database
        cur.execute("SELECT * from tasks, invocation_logs where tasks.uuid = '%s' and tasks.uuid = invocation_logs.task_uuid" % task_uuid)
        rows = cur.fetchall()

        # Get the most-recent status message
        for r in rows:
            exec_arn = r['arn']
            status = r['status']
            result = r['result']
            invocation_time = r['invocation']
        res = {'status': status, 'invocation_time': invocation_time}

        # If the task is using AWS step functions, check the status there
        # TODO (lw): I'm not sure what this does
        if exec_arn:
            # Check sfn for status
            sfn_client = boto3.client('stepfunctions')
            response = sfn_client.describe_execution(executionArn=exec_arn)
            status = response['status']
            res['status'] = status
            if 'output' in response:
                output = response['output']
                res['output'] = output

            # Update thes tatus in the database
            query = "UPDATE tasks set status = '%s' where uuid = '%s'" % (status, task_uuid)
            cur.execute(query)
            conn.commit()
        # Otherwise, this is an async request
        else:
            res = {'status': status, 'result': result, 'invocation_time': invocation_time}
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


# TODO (LW): Should we change this to the namespace format?
@api.route("/servables/<servable_uuid>/status", methods=['GET'])
def api_servable_status(servable_uuid):
    """
    Check the status of a servable.

    :param servable_uuid:
    :return:
    """

    # Check user authentication information
    user_id, user_name, short_name = _get_user(cur, conn, request.headers)
    if not user_name:
        abort(400, description="Error: You must be logged in to perform this function.")

    # Get the status of the servable from the database
    status = {}
    try:
        cur.execute("SELECT * from servables where uuid = '%s'" % servable_uuid)
        rows = cur.fetchall()

        # Get the most recent status
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

    Return:
        (str): JSON-encoded user name
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

    Args:
        servable_namespace (str): Namespace of servable
        servable_name (str): Name of the servable
    """
    user_id, user_name, short_name = _get_user(cur, conn, request.headers)
    if not user_name:
        abort(400, description="Error: You must be logged in to perform this function.")

    servable_uuid = _resolve_namespace_model(cur, conn, servable_namespace, servable_name)

    query = "select * from servables here uuid = '{0}' and author = '{1}'".format(servable_uuid, user_id)
    cur.execute(query)
    rows = cur.fetchall()
    if len(rows) == 0:
        return json.dumps({'status':'Failed to delete: permission denied or no servable found.'})

    query = "update servables set status = 'DELETED' where uuid = '{0}'".format(servable_uuid)
    try:
        cur.execute(query)
        conn.commit()
    except Exception as e:
        print(e)
        return json.dumps({"InternalError": e})

    return json.dumps({'status': 'done'})

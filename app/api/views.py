import boto3
import json
import uuid
import time
import os
from config import _load_dlhub_client
from .utils import (_get_user, _start_flow, _resolve_namespace_model, _get_dlhub_file_from_github,
                    create_presigned_post)
from flask import Blueprint, request, abort, jsonify, send_file
from werkzeug.utils import secure_filename

from config import (_get_db_connection, PUBLISH_FLOW_ARN, PUBLISH_REPO_FLOW_ARN)

conn, cur = _get_db_connection()

# Flask
api = Blueprint("api", __name__)

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


@api.route("/publish/signed_url", methods=['GET'])
def get_signed_url():
    """Get a signed URL for S3. This is used to upload large files out-of-band
    of the containerization process.

    Returns:
        str: JSON-encoded signed url
    """

    # Create a uuid name for the zip file
    tmp_uuid = uuid.uuid4()
    objname = "container_uploads/" + str(tmp_uuid)[:8] + ".zip"

    # signed_url = create_presigned_post('dlhub-anl', object_name=objname, conditions=[{"acl": "public-read"}])
    signed_url = create_presigned_post('dlhub-anl', object_name=objname)

    response = {'status': 'COMPLETED'}
    final_http_status = 200
    try:
        response['url'] = signed_url['url']
        response['fields'] = signed_url['fields']
    except Exception as e:
        # Failed to create a signed URL
        print(e)
        response = {'status': 'FAILED'}
        final_http_status = 500

    return jsonify(response), final_http_status


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

import psycopg2.extras
import globus_sdk
import psycopg2
import os

SECRET_KEY = os.environ.get('secret_key')
GLOBUS_KEY = os.environ.get('globus_key')
GLOBUS_CLIENT = os.environ.get('globus_client')

GIT_TOKEN = os.environ.get('git_token')

DB_HOST = os.environ.get('db_host')
DB_USER = os.environ.get('db_user')
DB_NAME = os.environ.get('db_name')
DB_PASSWORD = os.environ.get('db_password')

PUBLISH_FLOW_ARN = 'arn:aws:states:us-east-1:039706667969:stateMachine:DLHubIngestModel-3'
PUBLISH_REPO_FLOW_ARN = 'arn:aws:states:us-east-1:039706667969:stateMachine:DLHubIngestModel-4'

_prod = True


def _get_db_connection():
    """
    Establish a database connection
    """
    con_str = "dbname={dbname} user={dbuser} " \
              "password={dbpass} host={dbhost}".format(dbname=DB_NAME, dbuser=DB_USER,
                                                       dbpass=DB_PASSWORD, dbhost=DB_HOST)

    conn = psycopg2.connect(con_str)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    return conn, cur


def _load_dlhub_client():
    """
    Create an AuthClient for the portal
    """
    if _prod:
        app = globus_sdk.ConfidentialAppAuthClient(GLOBUS_CLIENT,
                                                   GLOBUS_KEY)
    else:
        app = globus_sdk.ConfidentialAppAuthClient('', '')
    return app

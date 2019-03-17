import psycopg2.extras
import globus_sdk
import psycopg2
import os

# GlobusAuth-related secrets
SECRET_KEY = os.environ.get('secret_key')
GLOBUS_KEY = os.environ.get('globus_key')
GLOBUS_CLIENT = os.environ.get('globus_client')

# GitHub-related secrets
GIT_KEY = os.environ.get('git_token')

# Secrets related to the servable information database
DB_HOST = os.environ.get('db_host')
DB_USER = os.environ.get('db_user')
DB_NAME = os.environ.get('db_name')
DB_PASSWORD = os.environ.get('db_password')

# Connections to the AWS publication work flows
PUBLISH_FLOW_ARN = 'arn:aws:states:us-east-1:039706667969:stateMachine:DLHubIngestModel-3'
PUBLISH_REPO_FLOW_ARN = 'arn:aws:states:us-east-1:039706667969:stateMachine:DLHubIngestModel-4'

# Whether this server is the production DLHub server
_prod = True


def _get_db_connection():
    """Establish a database connection

    Returns:
        conn: Connection to database
        cur: Active cursor for querying the databases
    """
    con_str = "dbname={dbname} user={dbuser} password={dbpass} host={dbhost}".format(dbname=DB_NAME, dbuser=DB_USER,
                                                                                     dbpass=DB_PASSWORD, dbhost=DB_HOST)

    conn = psycopg2.connect(con_str)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    return conn, cur


def _load_dlhub_client():
    """Create an AuthClient for the portal

    No credentials are used if the server is not production

    Returns:
        (globus_sdk.ConfidentialAppAuthClient): Client used to perform GlobusAuth actions
    """
    if _prod:
        app = globus_sdk.ConfidentialAppAuthClient(GLOBUS_CLIENT,
                                                   GLOBUS_KEY)
    else:
        app = globus_sdk.ConfidentialAppAuthClient('', '')
    return app

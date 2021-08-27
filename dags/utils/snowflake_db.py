from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from contextlib import closing
from airflow.models import Connection
from airflow import settings
import logging
from airflow.models import Variable

SNOWFLAKE_CONN_ID = Variable.get('env_snowflake_connection')
SNOWFLAKE_CONN_TYPE = Variable.get('env_connection_type')
SNOWFLAKE_HOST = Variable.get('env_snowflake_host')
SNOWFLAKE_LOGIN = Variable.get('env_snowflake_login')
SNOWFLAKE_PWD = Variable.get('env_snowflake_password')
SNOWFLAKE_SCHEMA = Variable.get('env_admin_schema')
SNOWFLAKE_EXTRA = Variable.get('env_snowflake_extra')


def create_airflow_connection():
    conn = Connection(
        conn_id=SNOWFLAKE_CONN_ID,
        conn_type=SNOWFLAKE_CONN_TYPE,
        host=SNOWFLAKE_HOST,
        login=SNOWFLAKE_LOGIN,
        password=SNOWFLAKE_PWD,
        schema=SNOWFLAKE_SCHEMA,
        extra=SNOWFLAKE_EXTRA
    )
    session = settings.Session()
    conn_name = session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()

    if str(conn_name) == str(SNOWFLAKE_CONN_ID):
        return logging.info(f"Connection {SNOWFLAKE_CONN_ID} already exists")

    session.add(conn)
    session.commit()
    logging.info(Connection.log_info(conn))
    logging.info(f'Connection {SNOWFLAKE_CONN_ID} is created')

def execute_snowflake_fetchall(query,with_cursor=False):
    hook_connection = SnowflakeHook(
        snowflake_conn_id=SNOWFLAKE_CONN_ID
    )

    with closing(hook_connection.get_conn()) as conn:
        with closing(conn.cursor()) as cur:
            cur.execute(query)
            result = cur.fetchall()
            if with_cursor:
                return (result, cur)
            else:
                return result

def execute_snowflake_fetchone(query,with_cursor=False):

    hook_connection = SnowflakeHook(
        snowflake_conn_id=SNOWFLAKE_CONN_ID
    )

    with closing(hook_connection.get_conn()) as conn:
        with closing(conn.cursor()) as cur:
            cur.execute(query)
            result = cur.fetchone()
            if with_cursor:
                return (result, cur)
            else:
                return result
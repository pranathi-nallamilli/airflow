from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from contextlib import closing
SNOWFLAKE_CONN_ID = 'snowflake_connection'

def execute_snowflake_fetchall(query,with_cursor=False):
    """Execute snowflake query."""
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
    """Execute snowflake query."""
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
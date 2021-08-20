from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from datetime import datetime

SNOWFLAKE_CONN_ID = 'snowflake_conn'
SLACK_CONN_ID = 'my_slack_conn'
# TODO: should be able to rely on connection's schema, but currently param required by S3ToSnowflakeTransfer
SNOWFLAKE_SCHEMA = 'schema_name'
SNOWFLAKE_STAGE = 'stage_name'
SNOWFLAKE_WAREHOUSE = 'warehouse_name'
SNOWFLAKE_DATABASE = 'database_name'
SNOWFLAKE_ROLE = 'role_name'
SNOWFLAKE_SAMPLE_TABLE = 'sample_table'
S3_FILE_PATH = '</path/to/file/sample_file.csv'

# SQL commands
CREATE_TABLE_SQL_STRING = (
    f"CREATE OR REPLACE TRANSIENT TABLE {SNOWFLAKE_SAMPLE_TABLE} (name VARCHAR(250), id INT);"
)
SQL_INSERT_STATEMENT = f"INSERT INTO {SNOWFLAKE_SAMPLE_TABLE} VALUES ('name', %(id)s)"
SQL_LIST = [SQL_INSERT_STATEMENT % {"id": n} for n in range(0, 10)]
SQL_MULTIPLE_STMTS = "; ".join(SQL_LIST)
SNOWFLAKE_SLACK_SQL = f"SELECT name, id FROM {SNOWFLAKE_SAMPLE_TABLE} LIMIT 10;"
SNOWFLAKE_SLACK_MESSAGE = (
    "Results in an ASCII table:\n```{{ results_df | tabulate(tablefmt='pretty', headers='keys') }}```"
)

default_args = {
    'start_date': datetime(2020, 1, 1),
    'owner': 'Airflow',
    'email': 'owner@test.com'
}


with DAG(dag_id='parallel_dag', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:
    
    # Tasks dynamically generated 

    task_5 = BashOperator(
        task_id='task_5',
        bash_command='docker run  cdm-docker-dev-local.repo.mgnt.in/hxp-cdm-file-spliter-py:14 /opt/airflow/dags/testfile.csv /opt/airflow/dags/required_fields.csv')


    task_5
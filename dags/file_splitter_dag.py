from os import sep
import sys
sys.path.insert(0, '/opt/airflow/dags/utils/')
from utils import mapping_and_validation as mv
from utils import get_schema as gc
from utils import data_catalog_lookup as dc
from utils import snowflake_db 
from utils import read_ds_dt
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.operators.bash_operator import BashOperator

default_args = {
    'start_date': datetime(2020, 1, 1),
    'owner': 'Airflow',
    'email': 'owner@test.com',
    'schedule_interval': '@daily'
}

command = 'python /opt/airflow/dags/file_splitter_console/hxp_ms_pii_file_splitter/main.py'
command+=' /opt/airflow/dags/utils/testfiles/splitter_testfile.csv'
command+=' /opt/airflow/dags/utils/testfiles/required_fields.csv'
with DAG(dag_id='file_splitter_dag', default_args=default_args, catchup=False) as dag:

    file_splitter = BashOperator(task_id='file_splitter', 
        bash_command=  command)

    file_splitter
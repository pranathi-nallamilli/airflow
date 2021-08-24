import sys
sys.path.insert(0, '/opt/airflow/dags/utils/')
from utils import mapping_and_validation as mv
from utils import get_schema as gc
from utils import data_catalog_lookup as dc
from utils import snowflake_db 
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

azure = WasbHook(wasb_conn_id='azure_blob')

default_args = {
    'start_date': datetime(2020, 1, 1),
    'owner': 'Airflow',
    'email': 'owner@test.com',
    'schedule_interval': '@daily'
}

container = 'archive'
blob = 'batch-ingestion/NETSUITE/CUSTOMERS/2021/07/12_csv_new_ds_withvalue_RID715997_1_T20210705_122729_853.csv'

def read_blob_list():
    file = azure.read_file(container,blob)
    print(file)

def check_connection():
 return(azure.check_for_blob(container,blob))

with DAG(dag_id='azure_dag', default_args=default_args, catchup=False) as dag:

    check_connection_opr = PythonOperator(task_id='connection',
                        python_callable=check_connection,
                        dag=dag)

    print_blob_list = PythonOperator(
    task_id='get_blob_list',
    python_callable=read_blob_list,
    dag=dag)

    check_connection_opr  >>  print_blob_list
    
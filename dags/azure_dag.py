from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from csv import reader
import pandas as pd

azure = WasbHook(wasb_conn_id='azure_blob')

default_args = {
    'start_date': datetime(2020, 1, 1),
    'owner': 'Airflow',
    'email': 'owner@test.com',
    'schedule_interval': '@daily'
}

container = 'archive'
blob = 'batch-ingestion/NETSUITE/CUSTOMERS/2021/07/12_csv_new_ds_withvalue_RID715997_1_T20210705_122729_853.csv'


azure = WasbHook(wasb_conn_id='azure_blob')
container = 'archive'
blob = 'batch-ingestion/NETSUITE/CUSTOMERS/2021/07/12_csv_new_ds_withvalue_RID715997_1_T20210705_122729_853.csv'

def azure_connection():
 return(azure.check_for_blob(container,blob))

def read_azure_blob_file():
    file = azure.read_file(container,blob)
    print(file)
    #write file locally
    df = pd.DataFrame(file)
    df.to_csv('/opt/airflow/dags/utils/testfiles/airflow_test_file.csv', index=False)
    

with DAG(dag_id='azure_dag', default_args=default_args, catchup=False) as dag:

    #check aure connection task
    check_azure_connection = PythonOperator(task_id='azure_connection',
                                python_callable=azure_connection,
                                dag=dag)

    save_azure_blob = PythonOperator(task_id='save_azure_blob',
                                python_callable=read_azure_blob_file,
                                dag=dag)

    check_azure_connection  >>  save_azure_blob
    
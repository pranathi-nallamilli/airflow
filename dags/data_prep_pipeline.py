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
from csv import reader
import pandas as pd

# azure = WasbHook(wasb_conn_id='azure_blob')
# container = 'archive'
# blob = 'batch-ingestion/NETSUITE/CUSTOMERS/2021/07/12_csv_new_ds_withvalue_RID715997_1_T20210705_122729_853.csv'

# def azure_connection():
#  return(azure.check_for_blob(container,blob))

# def read_azure_blob_file():
#     file = azure.read_file(container,blob)
#     print(file)
#     #write file locally
#     df = pd.DataFrame(file)
#     df.to_csv('/opt/airflow/dags/utils/testfiles/airflow_test_file.csv', index=False)
    

default_args = {
    'start_date': datetime(2020, 1, 1),
    'owner': 'Airflow',
    'email': 'owner@test.com',
    'schedule_interval': '@daily'
}

with DAG(dag_id='data_prep_pipeline', default_args=default_args, catchup=False) as dag:

    check_snowflake_connection = PythonOperator(task_id='check_snowflake_connection',     
                             python_callable=snowflake_db.create_airflow_connection,    
                             dag=dag )

    #check aure connection task
    # check_azure_connection = PythonOperator(task_id='azure_connection',
    #                             python_callable=azure_connection,
    #                             dag=dag)

    # save_azure_blob = PythonOperator(task_id='save_azure_blob',
    #                             python_callable=read_azure_blob_file,
    #                             dag=dag)

    get_schema = PythonOperator(task_id='get_schema',     
                             python_callable=gc.initiate_get_schema,    
                             dag=dag )

    # put read ds dt task
    read_DS_DT = PythonOperator(
                            task_id="read_DS_DT_task", python_callable=read_ds_dt.read_DS_DT_operation
                            , dag=dag
                        )


    data_catalog_lookup_task = PythonOperator(task_id='data_catalog_lookup',     
                              python_callable=dc.lookup_data_catalog,    
                              dag=dag )

    mapping_and_validation = PythonOperator(task_id='mapping_and_validation',     
                             python_callable=mv.mapping_validation_initiate_operation,    
                             dag=dag )

    check_snowflake_connection  >> get_schema >> read_DS_DT >> data_catalog_lookup_task >> mapping_and_validation
    
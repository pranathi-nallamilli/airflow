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

    get_schema = PythonOperator(task_id='get_schema',     
                             python_callable=gc.initiate_get_schema,    
                             dag=dag )

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
    
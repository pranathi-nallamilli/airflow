import sys
sys.path.insert(0, '/opt/airflow/dags/utils/')
from utils import mapping_and_validation as mv
from utils import get_schema as gc
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2020, 1, 1),
    'owner': 'Airflow',
    'email': 'owner@test.com',
    'schedule_interval': '@daily'
}

with DAG(dag_id='mapping_snowflake', default_args=default_args, catchup=False) as dag:

    # Data Upload  Task 
    # mapping_and_validation = PythonOperator(task_id='mapping_and_validation',     
    #                          python_callable=mv.mapping_validation_initiate_operation,    
    #                          dag=dag )

    get_schema = PythonOperator(task_id='get_schema',     
                             python_callable=gc.snowflake_data_operation,    
                             dag=dag )

    get_schema #>> mapping_and_validation
    
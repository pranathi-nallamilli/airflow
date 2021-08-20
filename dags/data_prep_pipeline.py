import sys
sys.path.insert(0, '/opt/airflow/dags/utils/')
from utils import mapping_and_validation as mv
from utils import get_schema as gc
from utils import snowflake_db 
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

    mapping_and_validation = PythonOperator(task_id='mapping_and_validation',     
                             python_callable=mv.mapping_validation_initiate_operation,    
                             dag=dag )

    # get_schema = PythonOperator(task_id='get_schema',     
    #                          python_callable=gc.initiate_get_schema,    
    #                          dag=dag )

    check_snowflake_connection >> mapping_and_validation
    
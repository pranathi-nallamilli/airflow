from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from utils import azure_operations as az

default_args = {
    'start_date': datetime(2020, 1, 1),
    'owner': 'Airflow',
    'email': 'owner@test.com',
    'schedule_interval': '@daily'
}

with DAG(dag_id='azure_dag', default_args=default_args, catchup=False) as dag:

    #check aure connection task
    check_azure_connection = PythonOperator(task_id='azure_connection',
                            python_callable= az.azure_connection,
                            dag=dag)

    write_to_azure  = PythonOperator(task_id='write_to_azure',
                            python_callable= az.write_to_azure_blob,
                            dag=dag)

    check_azure_connection  >>  write_to_azure
    
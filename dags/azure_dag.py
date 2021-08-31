from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from utils import azure_operations as az
from airflow.models import Variable

default_args = {
    'start_date': datetime(2020, 1, 1),
    'owner': 'Airflow',
    'email': 'owner@test.com',
    'schedule_interval': '@daily'
}
with DAG(dag_id='azure_dag', default_args=default_args, catchup=False) as dag:

    create_azure_connection  = PythonOperator(task_id='create_azure_connection',
                            python_callable= az.create_connection_azure,
                            dag=dag)
    get_all_blobs = PythonOperator(task_id='get_all_blobs',
                            python_callable= az.get_blob_list,
                            op_kwargs={'path': '','recursive':True},
                            dag=dag)
    read_blob_file = PythonOperator(task_id='read_blob_file',
                            python_callable= az.read_azure_blob_file,
                            op_kwargs={'blob': Variable.get('v_container_files')},
                            dag=dag)
    create_azure_connection  >> get_all_blobs >> read_blob_file
    #  write_to_azure
    
"""DAG file for the master operations of data prep"""
from datetime import datetime

import utils.database_tasks as dt
import utils.initiate_task as it

from airflow import DAG
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    "start_date": datetime(2021, 1, 1),
    "owner": "Niranjan Nareshan",
    "email": "nnareshan@deloitte.com",
}

with DAG(
    dag_id="data_prep_master",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    connect_to_database = PythonOperator(
        task_id="connect_to_database",
        python_callable=dt.create_database_connection,
        dag=dag,
        do_xcom_push=False,
        trigger_rule="one_success",
    )

    get_config_data = PythonOperator(
        task_id="get_config_data",
        python_callable=dt.setup_config_data,
        dag=dag,
        do_xcom_push=False,
    )

    get_all_files = ShortCircuitOperator(
        task_id="get_all_files",
        python_callable=it.get_files_list,
        op_kwargs={"path": "{{ var.value.env_data_path }}input"},
        dag=dag,
    )

    # pylint: disable=pointless-statement
    (
        get_all_files
        >> connect_to_database
        >> get_config_data
    )

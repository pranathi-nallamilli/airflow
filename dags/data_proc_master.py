"""DAG file for the master operations of data proc"""
from data_prep_master import default_args
import utils.initiate_task as it

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator


with DAG(
    dag_id="data_proc_master",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    check_pending_data_prep = ShortCircuitOperator(
        task_id="check_pending_data_prep",
        python_callable=it.check_for_new_blobs,
        op_kwargs={"variable_name": "v_data_prep_input_files", "negate": True},
        dag=dag,
    )

    get_input_files = PythonOperator(
        task_id="get_input_files",
        python_callable=it.get_files_list,
        op_kwargs={
            "path": "{{ var.value.env_data_path }}"
            + "{{ var.value.env_microservices_processing_path }}"
            + "prep/",
            "variable_name": "v_data_proc_input_files"
        },
        dag=dag,
    )

    # pylint: disable=pointless-statement
    check_pending_data_prep >> get_input_files

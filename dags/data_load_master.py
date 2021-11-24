"""DAG file for the master operations of data load"""
from data_prep_master import default_args
import utils.initiate_task as it
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow import DAG


with DAG(
    dag_id="data_load_master",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    check_pending_data_proc = ShortCircuitOperator(
        task_id="check_pending_data_proc",
        python_callable=it.check_for_new_blobs,
        op_kwargs={"variable_name": "v_data_proc_input_files", "negate": True},
        dag=dag,
    )

    get_data_load_input_files = PythonOperator(
        task_id="get_data_load_input_files",
        python_callable=it.get_files_list,
        op_kwargs={
            "path": "{{ var.value.env_data_path }}"
            + "{{ var.value.env_microservices_processing_path }}"
            + "proc/",
            "variable_name": "v_data_load_input_files"
        },
        dag=dag,
    )

    # pylint: disable=pointless-statement
    check_pending_data_proc >> get_data_load_input_files

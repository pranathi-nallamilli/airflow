"""DAG file that runs the Data load child process"""
from data_prep_master import default_args
import utils.initiate_task as it
import utils.get_tasks as gt

from airflow import DAG
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
with DAG(
    dag_id="data_load_child",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    check_for_new_data_load_input_files = ShortCircuitOperator(
        task_id="check_for_new_data_load_input_files",
        python_callable=it.check_for_new_blobs,
        op_kwargs={"variable_name": "v_data_load_input_files"},
        dag=dag,
    )

    with TaskGroup("data_load_tasks") as data_load_tasks:

        initiate_data_load_env = PythonOperator(
            task_id="initiate_data_load_env",
            python_callable=it.initiate_data_load_child,
            dag=dag,
        )

        data_load_process = gt.get_kubernetes_pod_operator(
            dag=dag,
            namespace="airflow",
            image="{{ var.value.env_data_load_image }}",
            args=[
                  "-conf",
                "{{ var.value.env_data_path }}{{ var.value.env_temp_processing_path }}"
                + "variables_{{ dag_run.id }}.json",
            ],
            task_id="data_load_process"
        )

        reset_data_load_blob_task = gt.get_reset_task(
            dag=dag,
            task_id="reset_data_load_blob_task",
            blob_index="{{ dag_run.id }}",
            variable_name="v_data_load_input_files",
            input_file_variable="v_blob_path"
        )

        cleanup_data_load_task = gt.get_cleanup_task(
            dag=dag,
            task_id="cleanup_data_load_task"
        )

        # pylint: disable=pointless-statement
        (
            initiate_data_load_env
            >> data_load_process
            >> reset_data_load_blob_task
            >> cleanup_data_load_task
        )

    # pylint: disable=pointless-statement
    check_for_new_data_load_input_files >> [data_load_tasks]

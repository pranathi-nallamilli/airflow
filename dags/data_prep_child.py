"""DAG file that runs the Data Prep child process"""
from data_prep_master import default_args
# import utils.cleanup_tasks as ct
import utils.initiate_task as it
import utils.get_tasks as gt
import utils.file_operations as fo

from airflow import DAG
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

with DAG(
    dag_id="data_prep_child",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    check_for_new_blobs_task = ShortCircuitOperator(
        task_id="check_for_new_blobs_task",
        python_callable=it.check_for_new_blobs,
        op_kwargs={"variable_name": "v_data_prep_input_files"},
        dag=dag,
    )

    with TaskGroup("data_prep_tasks") as data_prep_tasks:

        initiate_environment = PythonOperator(
            task_id="initiate_environment",
            python_callable=it.initiate_data_prep_child,
            dag=dag,
        )

        is_cleanse_enabled = BranchPythonOperator(
            task_id='is_cleanse_enabled',
            python_callable=gt.check_ms_enabled,
            op_kwargs={
                "blob_index": "{{ dag_run.id }}",
                "microservice": "CLEANSE",
                "prefix": "data_prep_tasks."
            },
            dag=dag
        )

        cleanse_task = gt.get_kubernetes_pod_operator(
            dag=dag,
            namespace="airflow",
            image="{{ var.value.env_cleanse_image }}",
            args=[
                "-i",
                "{{ var.value.env_data_path }}{{ var.value.env_temp_processing_path }}"
                + "cleanse_input_{{ dag_run.id }}.csv",
                "-o",
                "{{ var.value.env_data_path }}{{ var.value.env_temp_processing_path }}"
                + "cleanse_output_{{ dag_run.id }}.csv"
            ],
            task_id="cleanse_task",
        )

        skipping_cleanse = DummyOperator(task_id='skipping_cleanse')

        rename_output_of_cleanse = PythonOperator(
            task_id="rename_output_of_cleanse",
            python_callable=fo.rename_files_for_ms,
            op_kwargs={
                "blob_index": "{{ dag_run.id }}",
                "microservice": "CLEANSE",
                "is_input": False
            },
            trigger_rule=TriggerRule.ALL_DONE,
            dag=dag,
        )

        data_prep_process = gt.get_kubernetes_pod_operator(
            dag=dag,
            namespace="airflow",
            image="{{ var.value.env_data_prep_image }}",
            args=[
                "{{ var.value.env_data_path }}{{ var.value.env_temp_processing_path }}"
                + "variables_{{ dag_run.id }}.json ",
            ],
            task_id="data_prep_process"
        )

        # delete_blob_task = PythonOperator(
        #     task_id="delete_blob_task",
        #     python_callable=ct.delete_blob_task,
        #     dag=dag,
        #     trigger_rule=TriggerRule.ALL_SUCCESS,
        # )

        reset_data_prep_blob_task = gt.get_reset_task(
            dag=dag,
            task_id="reset_data_prep_blob_task",
            blob_index="{{ dag_run.id }}",
            variable_name="v_data_prep_input_files",
            input_file_variable="v_blob_path"
        )

        cleanup_data_prep_task = gt.get_cleanup_task(
            dag=dag,
            task_id="cleanup_data_prep_task"
        )

        # pylint: disable=pointless-statement
        (
            initiate_environment
            >> is_cleanse_enabled
            >> [cleanse_task, skipping_cleanse]
            >> rename_output_of_cleanse
            >> data_prep_process
            # >> delete_blob_task
            >> reset_data_prep_blob_task
            >> cleanup_data_prep_task
        )

    # pylint: disable=pointless-statement
    check_for_new_blobs_task >> [data_prep_tasks]

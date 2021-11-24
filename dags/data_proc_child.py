"""DAG file that runs the Data Proc child process"""
from data_prep_master import default_args
import utils.file_operations as fo
import utils.initiate_task as it
import utils.get_tasks as gt

from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

with DAG(
    dag_id="data_proc_child",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    check_for_new_input_files_task = ShortCircuitOperator(
        task_id="check_for_new_input_files_task",
        python_callable=it.check_for_new_blobs,
        op_kwargs={"variable_name": "v_data_proc_input_files"},
        dag=dag,
    )

    with TaskGroup("data_proc_tasks") as data_proc_tasks:

        initiate_data_proc_env = PythonOperator(
            task_id="initiate_data_proc_env",
            python_callable=it.initiate_data_proc_child,
            dag=dag,
        )

        is_convert_enabled = BranchPythonOperator(
            task_id='is_convert_enabled',
            python_callable=gt.check_ms_enabled,
            op_kwargs={
                "blob_index": "{{ dag_run.id }}",
                "microservice": "CONVERT",
                "prefix": "data_proc_tasks."
            },
            dag=dag
        )

        convert_task = gt.get_kubernetes_pod_operator(
            dag=dag,
            namespace="airflow",
            image="{{ var.value.env_convert_image }}",
            args=[
                "-i",
                "{{ var.value.env_data_path }}{{ var.value.env_temp_processing_path }}"
                + "convert_input_{{ dag_run.id }}.csv",
                "-o",
                "{{ var.value.env_data_path }}{{ var.value.env_temp_processing_path }}"
                + "convert_output_{{ dag_run.id }}.csv"
            ],
            task_id="convert_task",
        )

        rename_output_of_convert = PythonOperator(
            task_id="rename_output_of_convert",
            python_callable=fo.rename_files_for_ms,
            op_kwargs={
                "blob_index": "{{ dag_run.id }}",
                "microservice": "CONVERT",
                "is_input": False
            },
            trigger_rule=TriggerRule.ALL_DONE,
            dag=dag,
        )

        skipping_convert = DummyOperator(task_id='skipping_convert')

        is_cass_enabled = BranchPythonOperator(
            task_id='is_cass_enabled',
            python_callable=gt.check_ms_enabled,
            op_kwargs={
                "blob_index": "{{ dag_run.id }}",
                "microservice": "CASS",
                "prefix": "data_proc_tasks."
            },
            dag=dag
        )

        cass_task = gt.get_kubernetes_pod_operator(
            dag=dag,
            namespace="airflow",
            image="{{ var.value.env_cass_image }}",
            args=[
                "-i",
                "{{ var.value.env_data_path }}{{ var.value.env_temp_processing_path }}"
                + "cass_input_{{ dag_run.id }}.csv",
                "-o",
                "{{ var.value.env_data_path }}{{ var.value.env_temp_processing_path }}"
                + "cass_output_{{ dag_run.id }}.csv"
            ],
            task_id="cass_task",
            extra_volumes=[gt.volume_pw],
            extra_volume_mounts=[gt.volume_mount_pw]
        )

        rename_output_of_cass = PythonOperator(
            task_id="rename_output_of_cass",
            python_callable=fo.rename_files_for_ms,
            op_kwargs={"blob_index": "{{ dag_run.id }}", "microservice": "CASS", "is_input": False},
            trigger_rule=TriggerRule.ALL_DONE,
            dag=dag,
        )

        skipping_cass = DummyOperator(task_id='skipping_cass')

        is_pinning_enabled = BranchPythonOperator(
            task_id='is_pinning_enabled',
            python_callable=gt.check_ms_enabled,
            op_kwargs={
                "blob_index": "{{ dag_run.id }}",
                "microservice": "PINNING",
                "prefix": "data_proc_tasks."
            },
            dag=dag
        )

        pinning_task = gt.get_kubernetes_pod_operator(
            dag=dag,
            namespace="airflow",
            image="{{ var.value.env_pinning_image }}",
            args=[
                "-i",
                "{{ var.value.env_data_path }}{{ var.value.env_temp_processing_path }}"
                + "pinning_input_{{ dag_run.id }}.csv",
                "-o",
                "{{ var.value.env_data_path }}{{ var.value.env_temp_processing_path }}"
                + "pinning_output_{{ dag_run.id }}.csv",
                "-s",
                "{{ var.value.mongo_conn_string }}",
                "-d",
                "pinning"
            ],
            task_id="pinning_task",
        )

        rename_output_of_pinning = PythonOperator(
            task_id="rename_output_of_pinning",
            python_callable=fo.rename_files_for_ms,
            op_kwargs={"blob_index": "{{ dag_run.id }}",
                       "microservice": "PINNING", "is_input": False},
            trigger_rule=TriggerRule.ALL_DONE,
            dag=dag,
        )

        skipping_pinning = DummyOperator(task_id='skipping_pinning')

        is_stitch_enabled = BranchPythonOperator(
            task_id='is_stitch_enabled',
            python_callable=gt.check_ms_enabled,
            op_kwargs={
                "blob_index": "{{ dag_run.id }}",
                "microservice": "STITCH",
                "prefix": "data_proc_tasks."
            },
            dag=dag
        )

        stitch_task = gt.get_kubernetes_pod_operator(
            dag=dag,
            namespace="airflow",
            image="{{ var.value.env_stitch_image }}",
            args=[
                "-i",
                "{{ var.value.env_data_path }}{{ var.value.env_temp_processing_path }}"
                + "stitch_input_{{ dag_run.id }}.csv",
                "-o",
                "{{ var.value.env_data_path }}{{ var.value.env_temp_processing_path }}"
                + "stitch_output_{{ dag_run.id }}.csv",
                "-m",
                "{{ var.value.env_data_path}}config/map.csv",
                "-s",
                "{{ var.value.mongo_conn_string }}",
                "-d",
                "stitch",
                "-c",
                "StitchCollection",
                "--stats",
                "{{ var.value.env_data_path}}config/stats.txt"
            ],
            task_id="stitch_task",
        )

        rename_output_of_stitch = PythonOperator(
            task_id="rename_output_of_stitch",
            python_callable=fo.rename_files_for_ms,
            op_kwargs={"blob_index": "{{ dag_run.id }}",
                       "microservice": "STITCH", "is_input": False},
            trigger_rule=TriggerRule.ALL_DONE,
            dag=dag,
        )

        skipping_stitch = DummyOperator(task_id='skipping_stitch')

        write_output_file = PythonOperator(
            task_id="write_output_file",
            python_callable=fo.copy_data_proc_output,
            dag=dag,
            trigger_rule=TriggerRule.ALL_DONE,
        )

        reset_data_proc_blob_task = gt.get_reset_task(
            dag=dag,
            task_id="reset_data_proc_blob_task",
            blob_index="{{ dag_run.id }}",
            variable_name="v_data_proc_input_files",
            input_file_variable="v_input_file_path"
        )

        cleanup_data_proc_task = gt.get_cleanup_task(
            dag=dag,
            task_id="cleanup_data_proc_task"
        )

        # pylint: disable=pointless-statement
        (
            initiate_data_proc_env
            >> is_convert_enabled
            >> [convert_task, skipping_convert]
            >> rename_output_of_convert
            >> is_cass_enabled
            >> [cass_task, skipping_cass]
            >> rename_output_of_cass
            >> is_pinning_enabled
            >> [pinning_task, skipping_pinning]
            >> rename_output_of_pinning
            >> is_stitch_enabled
            >> [stitch_task, skipping_stitch]
            >> rename_output_of_stitch
            >> write_output_file
            >> reset_data_proc_blob_task
            >> cleanup_data_proc_task
        )

    # pylint: disable=pointless-statement
    check_for_new_input_files_task >> [data_proc_tasks]

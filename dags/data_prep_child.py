"""DAG file that runs the file splitter child process"""
from data_prep_master import default_args
import utils.cleanup_tasks as ct
import utils.initiate_task as it
from kubernetes.client.models import (
    V1PersistentVolumeClaimVolumeSource,
    V1Volume,
    V1VolumeMount,
)

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

with DAG(
    dag_id="data_prep_child",
    default_args=default_args,
    # Choose either automatic or external triggered
    # schedule_interval="*/1 * * * *",
    schedule_interval=None,
    tags=["demo"],
    catchup=False,
) as dag:

    check_for_new_blobs_task = ShortCircuitOperator(
        task_id="check_for_new_blobs_task",
        python_callable=it.check_for_new_blobs,
        dag=dag,
    )

    with TaskGroup("data_prep_tasks") as data_prep_tasks:

        initiate_environment = PythonOperator(
            task_id="initiate_environment",
            python_callable=it.initiate_data_prep_child,
            dag=dag,
        )

        volume_mount = V1VolumeMount(
            name=Variable.get("env_pvc_data"),
            mount_path=Variable.get("env_data_path"),
            sub_path=None,
            read_only=False,
        )

        volume = V1Volume(
            name=Variable.get("env_pvc_data"),
            persistent_volume_claim=V1PersistentVolumeClaimVolumeSource(
                claim_name=Variable.get("env_pvc_data")
            ),
        )

        data_prep_process = KubernetesPodOperator(
            namespace="airflow",
            image="{{ var.value.env_data_prep_image }}",
            image_pull_policy="IfNotPresent",
            image_pull_secrets="jfrog-registry",
            cmds=["python"],
            arguments=[
                "-m",
                "{{ var.value.env_file_splitter_path }}",
                "{{ var.value.env_data_path }}processing/variables_{{ dag_run.id }}.json ",
            ],
            name="data_prep_process",
            task_id="data_prep_process",
            in_cluster=True,
            get_logs=True,
            is_delete_operator_pod=True,
            volumes=[volume],
            volume_mounts=[volume_mount],
            dag=dag,
        )

        delete_blob_task = PythonOperator(
            task_id="delete_blob_task",
            python_callable=ct.delete_blob_task,
            dag=dag,
            trigger_rule=TriggerRule.ALL_SUCCESS,
        )

        reset_blob_task = PythonOperator(
            task_id="reset_blob_task",
            python_callable=ct.reset_blob_task,
            dag=dag,
            trigger_rule=TriggerRule.ONE_FAILED,
        )

        cleanup_iteration_task = PythonOperator(
            task_id="cleanup_iteration_task",
            python_callable=ct.cleanup_iteration_task,
            dag=dag,
            trigger_rule=TriggerRule.ALL_DONE,
        )
        # pylint: disable=pointless-statement
        (
            initiate_environment
            >> data_prep_process
            >> delete_blob_task
            >> reset_blob_task
            >> cleanup_iteration_task
        )

    # pylint: disable=pointless-statement
    check_for_new_blobs_task >> [data_prep_tasks]

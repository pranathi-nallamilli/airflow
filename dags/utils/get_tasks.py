"""File containing methods to generate repeated tasks"""
import ast
from utils.file_operations import get_variable
import utils.cleanup_tasks as ct
import utils.file_operations as fo
from kubernetes.client.models import (
    V1PersistentVolumeClaimVolumeSource,
    V1Volume,
    V1VolumeMount,
)
from airflow.models.dag import DAG
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

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

volume_mount_pw = V1VolumeMount(
    name=Variable.get("env_pvc_pw"),
    mount_path=Variable.get("env_pw_path"),
    sub_path=None,
    read_only=False,
)

volume_pw = V1Volume(
    name=Variable.get("env_pvc_pw"),
    persistent_volume_claim=V1PersistentVolumeClaimVolumeSource(
        claim_name=Variable.get("env_pvc_pw")
    ),
)


def get_kubernetes_pod_operator(
    *,
    dag: DAG,
    namespace: str,
    image: str,
    args: list,
    task_id: str,
    commands: list = None,
    extra_volumes: list = None,
    extra_volume_mounts: list = None,
    trigger_rule: TriggerRule = TriggerRule.ALL_SUCCESS
):
    """Method to return a Kubernetes Pod Operator with the passed params applied"""

    return KubernetesPodOperator(
        namespace=namespace,
        image=image,
        image_pull_policy="IfNotPresent",
        image_pull_secrets="jfrog-registry",
        cmds=commands,
        arguments=args,
        name=task_id,
        task_id=task_id,
        in_cluster=True,
        get_logs=True,
        is_delete_operator_pod=True,
        volumes=[volume] + (extra_volumes if extra_volumes else []),
        volume_mounts=[volume_mount] + (extra_volume_mounts if extra_volumes else []),
        dag=dag,
        trigger_rule=trigger_rule,
        startup_timeout_seconds=1200
    )


def get_reset_task(
    dag,
    blob_index: str,
    task_id: str,
    variable_name: str,
    input_file_variable: str
):
    """Method to return the python operator to reset the file back into the backlog"""
    return PythonOperator(
        task_id=task_id,
        python_callable=ct.reset_blob_task,
        op_kwargs={"blob_index": blob_index,
                   "variable_name": variable_name,
                   "input_file_variable": input_file_variable},
        dag=dag,
        trigger_rule=TriggerRule.ONE_FAILED,
    )


def get_cleanup_task(dag, task_id: str):
    """Method to return the python operator to cleanup the temporary files after running"""
    return PythonOperator(
        task_id=task_id,
        python_callable=ct.cleanup_iteration_task,
        dag=dag,
        trigger_rule=TriggerRule.ALL_DONE,
    )


def check_ms_enabled(blob_index: str, microservice: str, prefix: str):
    """Method to check if a given microservice should be run for a pipeline or ̵"""
    v_microservices_list = get_variable(blob_index, "v_microservices_list")
    v_microservices_list = v_microservices_list + \
        (", CLEANSE" if ast.literal_eval(Variable.get("is_cleanse_enabled")) else "")
    if microservice in v_microservices_list:
        fo.rename_files_for_ms(blob_index, microservice, True)
        return prefix + microservice.lower() + "_task"
    return prefix + "skipping_" + microservice.lower()

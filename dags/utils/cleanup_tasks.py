"""File to run cleanup scripts after task completetion"""
import ast
import utils.file_operations as fo

from airflow.models import Variable


def cleanup_iteration_task(dag_run):
    """Method that deletes temporary files after DAG run"""
    blob_index = dag_run.id

    variable_path = fo.get_variable_path(blob_index)
    fo.delete_file(variable_path, "Variables")


def reset_blob_task(dag_run):
    """Method to reset the blob in the v_container_files variable when some task fails"""
    blob_index = dag_run.id
    blob = fo.get_variable(blob_index, "v_blob_path")
    v_container_files = Variable.get("v_container_files")
    blobs = ast.literal_eval(
        v_container_files) if v_container_files != "" else []
    blobs = ([blob] if blob != "" else []) + blobs
    Variable.set("v_container_files", blobs)


def delete_blob_task(dag_run):
    """Method to delete the original blob image after all the steps are successful"""
    if ast.literal_eval(Variable.get("emv_delete_source_blob")):
        blob_index = dag_run.id
        blob = f"{Variable.get('env_data_path')}input/{fo.get_variable(blob_index, 'v_blob_path')}"
        fo.delete_file(blob, "Input")

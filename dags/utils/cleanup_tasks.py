"""File to run cleanup scripts after task completetion"""
import ast
import utils.file_operations as fo

from airflow.models import Variable


def cleanup_iteration_task(dag_run):
    """Method that deletes temporary files after DAG run"""
    blob_index = dag_run.id

    variable_path = fo.get_variable_path(blob_index)
    fo.delete_file(variable_path, "Variables")


def reset_blob_task(blob_index: str, variable_name: str, input_file_variable: str):
    """Method to reset the blob in the files variable when some task fails"""
    blob = fo.get_variable(blob_index, input_file_variable)
    files = Variable.get(variable_name)
    blobs = ast.literal_eval(
        files) if files != "" else []
    blobs = ([blob] if blob != "" else []) + blobs
    Variable.set(variable_name, blobs)


# def delete_blob_task(dag_run):
#     """Method to delete the original blob image after all the steps are successful"""
#     if ast.literal_eval(Variable.get("env_delete_source_blob")):
#         blob_index = dag_run.id
#         blob = Variable.get('env_data_path')\
#             + Variable.get('env_pre_processing_path')\
#             + fo.get_variable(blob_index, 'v_blob_path')
#         fo.delete_file(blob, "Input")

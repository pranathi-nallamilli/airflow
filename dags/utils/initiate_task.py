"""File to initialize the files and environment for Data Prep"""
import os
import ast
import logging
import datetime

import utils.file_operations as fo
from utils.database_tasks import get_microservice_status

from airflow.models import Variable


def get_files_list(path: str, variable_name: str):
    """Method to get the list of files present in the input folder"""
    if not path == "" and not path.endswith("/"):
        path += "/"

    logging.info("Root path is %s", path)
    valid_files = []
    for root, _, files in os.walk(path):
        logging.info("Files are  %s", files)
        for file in files:
            if str(file).upper().endswith(("CSV", "JSON", "PARQUET")):
                valid_files.append(os.path.join(root, file).replace(path, ""))
    Variable.set(variable_name, valid_files)
    logging.info("Files are  %s", valid_files)
    return len(valid_files) > 0


def check_for_new_blobs(variable_name: str, negate: bool = False):
    """
    Method to check if new input files have been found and added in the
    given variable
    """
    files = Variable.get(variable_name)
    blobs = ast.literal_eval(files) if files != "" else []
    logging.info("New Blobs %s", blobs)
    has_files = (len(blobs) > 0)
    return not has_files if negate else has_files


def initiate_data_prep_child(dag_run):
    """
    Method to create the variables file for a particular run of Data Prep
    with initial environment values
    """
    blob_index = dag_run.id

    v_data_prep_input_files = ast.literal_eval(
        Variable.get("v_data_prep_input_files"))
    logging.info("Run Id %s", blob_index)
    v_blob_path = v_data_prep_input_files[0]
    rem_files = v_data_prep_input_files[1:]
    Variable.set(
        "v_data_prep_input_files", rem_files if len(rem_files) != 0 else ""
    )
    logging.info("Data prep started for file %s", v_blob_path)

    env_data_path = Variable.get("env_data_path")
    env_pre_processing_path = Variable.get("env_pre_processing_path")
    ms_folder = env_data_path + Variable.get("env_microservices_processing_path")

    state = {
        "v_last_microservice": "pre-processing",
        "v_blob_path": v_blob_path,
        "v_pre-processing_output": f"{env_data_path}{env_pre_processing_path}{v_blob_path}",
        "v_cleanse_output": f"{ms_folder}cleanse/{v_blob_path}",
        "env_config_file_path": f"{env_data_path}config/config_data.json",
        "env_src_fields_path": f"{env_data_path}config/src_fields.csv",
        "env_ms_output_path": f"{ms_folder}prep/",
        "env_nms_output_path":  f"{env_data_path}{Variable.get('env_nms_output_folder')}",
        "env_data_catalog_new_feed_blob": env_data_path
        + Variable.get('env_data_catalog_new_feed_blob'),
        "env_data_catalog_error_path": env_data_path + Variable.get('env_data_catalog_error_path'),
        "is_cleanse_enabled": Variable.get("is_cleanse_enabled"),
        "v_batch_ingestion_blob_output_path": "",
        "v_required_fields": "",
        "v_data_source_flag": "N",
        "v_data_type_flag": "N",
        "v_data_source": "",
        "v_data_type": "",
        "v_year_string": "",
        "v_month_string": "",
        "v_day_string": "",
        "v_batch_ingestion_filename": "",
        "v_feed_id": "",
        "v_pipeline": "",
        "v_pipeline_flow": "",
    }
    fo.write_to_json_file(fo.get_variable_path(blob_index), state)
    return blob_index


def initiate_data_proc_child(dag_run):
    """
    Method to create the variables file for a particular run of Data Proc
    with initial environment values
    """
    blob_index = dag_run.id

    v_data_proc_input_files = ast.literal_eval(
        Variable.get("v_data_proc_input_files"))
    logging.info("Run Id %s", blob_index)
    v_input_file_path: str = v_data_proc_input_files[0]
    rem_files = v_data_proc_input_files[1:]
    Variable.set(
        "v_data_proc_input_files", rem_files if len(rem_files) != 0 else ""
    )
    logging.info("Data proc started for file %s", v_input_file_path)

    v_pipeline = v_input_file_path.split("/")[-1].split("_")[0]

    v_microservices_list = get_microservice_status(v_pipeline)

    ms_folder_path = Variable.get("env_data_path")\
        + Variable.get("env_microservices_processing_path")

    current_date = datetime.datetime.now()
    day_string = str(current_date.day).zfill(2)
    month_string = str(current_date.month).zfill(2)
    year_string = str(current_date.year)
    timestamp = day_string + month_string + year_string

    state = {
        "v_last_microservice": "prep",
        "v_input_file_path": v_input_file_path,
        "v_pipeline": v_pipeline,
        "v_microservices_list": v_microservices_list,
        "v_output_file_path": f"{ms_folder_path}proc/{v_pipeline}_data_proc_{timestamp}.csv",

        "v_prep_output": f"{ms_folder_path}prep/{v_input_file_path}",

        "v_convert_output": f"{ms_folder_path}convert/{v_pipeline}_{blob_index}_{timestamp}.csv",

        "v_cass_output": f"{ms_folder_path}cass/{v_pipeline}_{blob_index}_{timestamp}.csv",

        "v_pinning_output": f"{ms_folder_path}pinning/{v_pipeline}_{blob_index}_{timestamp}.csv",

        "v_stitch_output": f"{ms_folder_path}stitch/{v_pipeline}_{blob_index}_{timestamp}.csv",
    }
    fo.write_to_json_file(fo.get_variable_path(blob_index), state)


def initiate_data_load_child(dag_run):
    """
    Method to create the variables file for a particular run of Data Load
    with initial environment values
    """

    v_data_load_input_files = ast.literal_eval(Variable.get("v_data_load_input_files"))
    logging.info("Run Id %s", dag_run.id)
    v_input_file_path: str = v_data_load_input_files[0]
    rem_files = v_data_load_input_files[1:]
    Variable.set("v_data_load_input_files", rem_files if len(rem_files) != 0 else "")
    logging.info("Data load started for file %s", v_input_file_path)

    ms_folder_path = Variable.get("env_data_path")\
        + Variable.get("env_microservices_processing_path")\
        + "proc/"

    state = {
        "v_blob_path": v_input_file_path,
        "sf_account": Variable.get("env_data_load_sf_account"),
        "sf_user": Variable.get("env_snowflake_login"),
        "sf_password": Variable.get("env_snowflake_password"),
        "mongo_uri": Variable.get("mongo_conn_string"),
        "mongo_user": Variable.get("env_mongo_user"),
        "mongo_password": Variable.get("env_mongo_password"),
        "input_file": ms_folder_path+v_input_file_path,
        "database": Variable.get("env_data_load_sf_database"),
        "ingestion_type": Variable.get("env_data_load_sf_type"),
        "schema": Variable.get("env_data_load_sf_schema"),
        "tablename": Variable.get("env_data_load_sf_table"),
        "truncate": Variable.get("env_data_load_sf_truncate"),
        "from_azure_file": "False",
        "collection": "",
        "file_path": "",
        "container_name": "",
    }
    fo.write_to_json_file(fo.get_variable_path(dag_run.id), state)

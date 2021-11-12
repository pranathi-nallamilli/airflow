"""File to initialize the files and environment for Data Prep"""
import os
import ast
import logging
import utils.file_operations as fo

from airflow.models import Variable


def get_files_list(path: str):
    """Method to get the list of files present in the input folder"""
    if not path == "" and not path.endswith("/"):
        path += "/"

    logging.info("Root path is %s", path)
    valid_files = []
    for root, _, files in os.walk(path):
        for file in files:
            if str(file).upper().endswith(("CSV", "JSON", "PARQUET")):
                valid_files.append(os.path.join(root, file).replace(path, ""))
    Variable.set("v_container_files", valid_files)
    return len(valid_files) > 0


def check_for_new_blobs():
    """
    Method to check if new input files have been found and added in the
    v_container_files variable
    """
    v_container_files = Variable.get("v_container_files")
    blobs = ast.literal_eval(
        v_container_files) if v_container_files != "" else []
    logging.info("New Blobs %s", blobs)
    return len(blobs) > 0


def initiate_data_prep_child(dag_run):
    """
    Method to create the variables file for a particular run
    with initial environment values
    """
    blob_index = dag_run.id

    v_container_files = ast.literal_eval(Variable.get("v_container_files"))
    logging.info("Run Id %s", blob_index)
    v_blob_path = v_container_files[0]
    v_container_files = v_container_files[1:]
    Variable.set(
        "v_container_files", v_container_files if len(
            v_container_files) != 0 else ""
    )
    logging.info("Data prep started for file %s", v_blob_path)

    env_data_path = Variable.get("env_data_path")

    state = {
        "v_blob_path": v_blob_path,
        "env_config_file_path": f"{env_data_path}config/config_data.json",
        "env_src_fields_path": f"{env_data_path}config/src_fields.csv",
        "env_microservices_processing_path": Variable.get(
            "env_microservices_processing_path"
        ),
        "env_data_catalog_new_feed_blob": Variable.get(
            "env_data_catalog_new_feed_blob"
        ),
        "env_data_catalog_error_path": Variable.get("env_data_catalog_error_path"),
        "v_batch_ingestion_blob_output_path": "",
        "v_required_fields": "",
        "v_data_source_flag": "N",
        "v_data_type_flag": "N",
        "v_data_source": "",
        "v_data_type": "",
        "v_batch_ingestion_file_path": "",
        "v_year_string": "",
        "v_month_string": "",
        "v_day_string": "",
        "v_batch_ingestion_filename": "",
        "v_feed_id": "",
        "v_pipeline": "",
        "v_pipeline_flow": "",
        "v_batch_ingestion_archive_new_feed_path": "",
    }
    fo.write_to_json_file(fo.get_variable_path(blob_index), state)

"""File that holds the scripts for file I/O operations"""
import json
import os
import logging
import shutil
from bson.json_util import dumps
from airflow.models import Variable


def write_config_file(file_path, data):
    """Method to write data into the config file"""
    with open(file_path, "w", encoding="utf-8") as file:
        file.write("[")
        for document in list(data):
            file.write(dumps(document))
            file.write(",")
        file.seek(0, os.SEEK_END)
        file.seek(file.tell() - 1, os.SEEK_SET)
        file.truncate()
        file.write("]")


def get_variable_path(blob_index):
    """Method to fetch the path of the variable file"""
    return f"{Variable.get('env_data_path')}temp/variables_{blob_index}.json"


def get_variable(blob_index: str, variable: str) -> str:
    """Method to read the value of a variable inside the variable file"""
    result = ""
    file_path = get_variable_path(blob_index)
    if file_path and os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as json_file:
            state = json.load(json_file)
            if variable in state:
                result = state[variable]
    return result


def set_variable(blob_index: str, variable: str, value: str):
    """Method to set the value of a variable inside the variable file"""
    variable_path = get_variable_path(blob_index)
    if variable_path and os.path.exists(variable_path):
        with open(variable_path, "r+", encoding="utf-8") as json_file:
            state = json.load(json_file)
            state[variable] = value
            json_file.seek(0)
            json.dump(state, json_file)
            json_file.truncate()


def write_to_json_file(path, data):
    """Method to write output to a json file"""
    with open(path, "w+", encoding="utf-8") as json_file:
        json.dump(data, json_file)


def delete_file(file_path, name):
    """Method to delete a given file"""
    if file_path and os.path.exists(file_path):
        os.remove(file_path)
        logging.info("%s file deleted", name)
    else:
        logging.info("%s does not exist", name)


def copy_data_proc_output(dag_run):
    """Method to copy the last MS output to output folder"""
    blob_index = dag_run.id
    last_ms = get_variable(blob_index, "v_last_microservice").lower()
    final_output_file = get_variable(blob_index, f"v_{last_ms}_output")
    v_output_file_path = get_variable(blob_index, "v_output_file_path")

    shutil.copyfile(final_output_file, v_output_file_path)


def rename_files_for_ms(blob_index: str, microservice: str, is_input: bool):
    """Method to rename the files before and after microservice"""

    last_ms = get_variable(blob_index, "v_last_microservice").lower()
    microservice = microservice.lower()

    env_data_path: str = Variable.get("env_data_path")
    temp_folder_path: str = env_data_path + Variable.get("env_temp_processing_path")

    input_file_path: str = get_variable(blob_index, f"v_{last_ms}_output")
    new_input_file_path: str = temp_folder_path + f"{microservice}_input_{blob_index}.csv"

    output_file_path: str = new_input_file_path.replace("input", "output")
    new_output_file_path: str = get_variable(blob_index, f"v_{microservice}_output")

    if is_input:
        if os.path.exists(input_file_path):
            os.rename(input_file_path, new_input_file_path)

    else:
        if os.path.exists(output_file_path):
            os.rename(output_file_path, new_output_file_path)
            set_variable(blob_index, "v_last_microservice", microservice)
        if os.path.exists(new_input_file_path):
            os.rename(new_input_file_path, input_file_path)

"""File that holds the scripts for file I/O operations"""
import json
import os
import logging
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
    return f"{Variable.get('env_data_path')}processing/variables_{blob_index}.json"


def get_variable(blob_index, variable):
    """Method to read the value of a variable inside the variable file"""
    result = ""
    file_path = get_variable_path(blob_index)
    if file_path and os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as json_file:
            state = json.load(json_file)
            result = state[variable]
    return result


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

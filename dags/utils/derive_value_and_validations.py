import re
import datetime
from airflow.models import Variable
import pandas as pd
from config_data import (
    v_data_source,
    v_data_type,
    run_history_id,
    v_iter_num,
    v_unq_id,
    v_sub_folder,
    v_error_flag,
    v_filename,
    v_base_folder,
    v_batch_ingestion_path,
    v_batch_ingestion_file_path,
    env_invalid_chars_path
)
# calling method
def derive_and_validate_file():
    print("Run History Id: " + str(run_history_id))
    print("Iteration Number: " + v_iter_num)
    print("Unique Id: " + v_unq_id)
    error_flag = True

    if v_error_flag == "N":
        error_flag = False
        print("Datasource: " + v_data_source)
        print("Datatype: " + v_data_type)

    file_ext = v_filename.split(".")

    if len(file_ext) > 1:
        file_ext = file_ext[-1]
        is_csv = file_ext.upper() == "CSV"

        if not error_flag and not is_csv:
            Variable.set("v_error_flag", "Y")
            error_flag = True
            Variable.set(
                "v_error_msg",
                f"File type/extension: {file_ext} not supported for file '{v_base_folder}{v_sub_folder}{v_filename}'",
            )
        # TODO: Check if can be removed by finding usages
        elif not error_flag and is_csv:
            Variable.set("v_field_seperator", ",")

    elif len(file_ext) == 1 and not error_flag:
        file_ext = ""
        Variable.set("v_error_flag", "Y")
        error_flag = True
        Variable.set(
            "v_error_msg",
            f"File Extension not found for file '{v_base_folder}{v_sub_folder}{v_filename}'",
        )

    current_date = datetime.datetime.now()
    year_string = str(current_date.year)
    month_string = str(current_date.month).zfill(2)
    Variable.set("v_year_string", year_string)
    Variable.set("v_month_string", month_string)

    v_batch_ingestion_filename = v_filename.replace("." + file_ext, "")

    if re.match("^[a-zA-Z0-9_]*$", v_batch_ingestion_filename):
        print("Input file name is valid: " + v_filename)
    else:
        dataframe = pd.read_csv(env_invalid_chars_path)
        invalid_chars = dataframe.values[0] 
        for ch in invalid_chars:
            v_batch_ingestion_filename = v_batch_ingestion_filename.replace(ch, "")
        v_batch_ingestion_filename = v_batch_ingestion_filename.strip("_")
        print(
            f"Warning: Input file name was not valid. File name converted to:'{v_batch_ingestion_filename}'"
        )

    Variable.set("v_batch_ingestion_filename", v_batch_ingestion_filename)
    Variable.set("v_batch_ingestion_file_path", v_sub_folder + v_filename)
    print(
        f"Batch Ingestion File Read path: {v_batch_ingestion_path + v_batch_ingestion_file_path}"
    )
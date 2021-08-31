from airflow.models import Variable
import sys
sys.path.insert(0, '/opt/airflow/dags/utils/')
from config_data import (
    v_sub_folder,
    v_filename,
    v_base_folder,
    #inputStorageAccount,
)

# Set Data Source an Data type from the path
def check_file_path():
    if v_sub_folder is None:
        Variable.set(v_sub_folder, "")

    file_path = v_base_folder + v_sub_folder + v_filename
    fields = (file_path).split("/")
    fields_count = len(fields)
    error = False
    error_message = ""
    data_source = None
    data_type = None
    print("fields", fields)
    print("Count ", fields_count)
    # When no sub folder found
    if fields_count < 3:
        error = True
        error_message = (
            f"DataSource and DataType subfolders not found for file:'{file_path}'"
        )
        data_source = "NA"
        data_type = "NA"

    # When one sub folder found
    elif fields_count == 3:
        error = True
        error_message = f"DataType subfolder not found for file:'{file_path}'"
        data_source = fields[1].upper()
        data_type = "NA"

    # When both sub folders found
    else:
        data_source = fields[1].upper()
        data_type = fields[2].upper()

        # When more sub folders found
        if fields_count > 4:
            error = True
            error_message = (
                f"File path '{file_path}' not supported.File path has extra subfolders."
            )

    Variable.set("v_data_source", data_source)
    Variable.set("v_data_type", data_type)
    print("Datasource: " + data_source)
    print("Datatype: " + data_type)

    
    if error:
        Variable.set("v_error_flag", "Y")
        Variable.set("v_error_msg", error_message)

    #TODO :Check usage of below variables
    # if data_source != "NA":
    #     Variable.set("v_fn_data_source", data_source)

    # if data_type != "NA":
    #     Variable.set("v_fn_data_type", data_type)

    # Variable.set(
    #     "v_fn_batch_ingestion_file_path",
    #     inputStorageAccount + v_base_folder + v_sub_folder,
    # )
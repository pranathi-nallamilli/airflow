from airflow.models import Variable
from airflow import AirflowException
import pandas as pd
import sys
sys.path.insert(0, '/opt/airflow/dags/utils/')
from utils import snowflake_db
from config_data import (
    v_data_source,
    v_data_type,
    v_data_source_flag,
    v_data_type_flag,
    v_field_seperator,
    env_file_path
)

# First check if csv has both ds and dt columns.
# If yes, then file doesn't need to be updated
# If no, then add the required columns and their pre existing value(from env variable)
# Move to checking distinct combination.

def set_new_variable_values(ds_dt_combinations):
    # if file contains more than one combination of data source and data type then send to error container
    if len(ds_dt_combinations) > 1:
        raise AirflowException(
            f"More than one combinations of Data Source and Data Type found in the file!!\nFilename :"
        )

    else:
        # Check if the values for data source or data type fields are missing
        ds_value_flag = ds_dt_combinations[0][0] != ""
        dt_value_flag = ds_dt_combinations[0][1] != ""

        # if either of the data source or data type field is null send the file to error container
        if not ds_value_flag and not dt_value_flag:
            raise AirflowException(
                f"The values for Data Source and Data Type fields are missing: Verify that the file contains the values for these fields!!\nFilename : {v_batch_ingestion_filename}"
            )
        elif not ds_value_flag:
            raise AirflowException(
                f"The value for Data Source field is missing: Verify that the file contains the value for Data Source field!!\nFilename : {v_batch_ingestion_filename}"
            )
        elif not dt_value_flag:
            raise AirflowException(
                f"The value for Data Type field is missing: Verify that the file contains the value for Data Type field!!\nFilename : {v_batch_ingestion_filename}"
            )

        # when both data source and data type fields are present
        else:
            # store data source and data type values to be used later in the data catalog lookup

            print("Storing v_data_source as ", str(ds_dt_combinations[0][0]).upper())
            Variable.set("v_data_source", str(ds_dt_combinations[0][0]).upper())

            print("Storing v_data_type as ", str(ds_dt_combinations[0][1]).upper())
            Variable.set("v_data_type", str(ds_dt_combinations[0][1]).upper())

def AddRequiredColumnsToFile():

    dataframe = pd.read_csv(env_file_path, delimiter=v_field_seperator)
    if v_data_source_flag.upper() == "N" and v_data_type_flag.upper() == "N":
        dataframe.insert(len(dataframe.columns),column='DATA_SOURCE',value = v_data_source.upper())
        dataframe.insert(len(dataframe.columns),column='DATA_TYPE',value = v_data_type.upper())

    elif v_data_source_flag.upper() == "Y" and v_data_type_flag.upper() == "N":
        dataframe.insert(len(dataframe.columns),column='DATA_TYPE',value = v_data_type.upper())
    else:
        dataframe.insert(dataframe.columns.get_loc('DATA_TYPE'),column='DATA_SOURCE',value = v_data_source.upper())

    print(dataframe)
    dataframe.to_csv(env_file_path,index=False)

def read_DS_DT_operation():
    #if csv doesn't have any of the ds or dt columns
    
    if(v_data_source_flag == "N" or v_data_type_flag == "N"):
        AddRequiredColumnsToFile()

    if v_data_source_flag == "Y" or v_data_type_flag == "Y":
        dataframe = pd.read_csv(env_file_path, delimiter=v_field_seperator)
        print(dataframe)
        # Get distinct combinations of Data Source and Data Type fields
        ds_dt_combinations =  dataframe.drop_duplicates(subset=['DATA_SOURCE','DATA_TYPE'])[['DATA_SOURCE','DATA_TYPE']].values
        print("Mandatory Columns: ", ds_dt_combinations)
        set_new_variable_values(ds_dt_combinations)

    else:
        print(
            "Skipping distinct data source and data type lookup as both fields are missing from the file."
        )
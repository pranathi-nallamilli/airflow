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
    v_batch_ingestion_stage,
    v_batch_ingestion_stage_file_path,
    v_schema_file,
    v_file_ext,
    v_file_format,
    v_batch_ingestion_out_view,
    v_etl_stage_database,
    v_etl_stage_schema,
    v_batch_ingestion_filename,
)


def create_view(view_name, file_path, headers, file_format, ds_flag, dt_flag):
    view_query = f"CREATE OR REPLACE VIEW {view_name} AS (SELECT "

    for pos, header in enumerate(headers, start=1):
        if v_file_ext.upper() == "CSV":
            view_query += f"${str(pos)} {str(header.upper())}, "
        else:
            view_query += f"${str(1)}:{str(header)} {str(header.upper())}, "

    # Create new columns for data source and data type if they are not present in the file itself
    if ds_flag.upper() == "N":
        view_query += f"'{str(v_data_source)}' {str('DATA_SOURCE')}, "

    if dt_flag.upper() == "N":
        view_query += f"'{str(v_data_type)}' {str('DATA_TYPE')}, "

    view_query = (
        f"{view_query[0:-2]} FROM '@{file_path}'(file_format => {file_format}) t);"
    )

    print(view_query)

    output = snowflake_db.execute_snowflake_fetchone(view_query)

    print(output)


def get_headers_from_schema():

    dataframe = pd.read_csv(v_schema_file, delimiter=",")
    headers = list(dataframe[dataframe.columns[0]])

    print(headers)

    return headers


def set_new_variable_values(ds_dt_combinations):
    # if file contains more than one combination of data source and data type then send to error container
    if len(ds_dt_combinations) > 1:
        raise AirflowException(
            f"More than one combinations of Data Source and Data Type found in the file!!\nFilename : {v_batch_ingestion_filename}"
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


def read_DS_DT_operation():
    # Y denotes we need to read the DS or DT form one of the file columns instead of from the environment variable
    if v_data_source_flag == "Y" or v_data_type_flag == "Y":
        v_file_path = v_batch_ingestion_stage + v_batch_ingestion_stage_file_path

        headers = get_headers_from_schema()

        create_view(
            v_batch_ingestion_out_view,
            v_file_path,
            headers,
            v_file_format,
            v_data_source_flag,
            v_data_type_flag,
        )

        # Query in the stage view for combination of Data Source and Data Type fields
        ds_dt_query = f"SELECT DISTINCT upper(DATA_SOURCE) DATA_SOURCE, upper(DATA_TYPE) DATA_TYPE FROM {v_etl_stage_database}.{v_etl_stage_schema}.{v_batch_ingestion_out_view}"

        print(ds_dt_query)

        # executing query
        ds_dt_combinations = snowflake_db.execute_snowflake_fetchall(ds_dt_query)

        print("Mandatory Columns: ", ds_dt_combinations)

        set_new_variable_values(ds_dt_combinations)

    else:
        print(
            "Skipping distinct data source and data type lookup as both fields are missing from the file."
        )
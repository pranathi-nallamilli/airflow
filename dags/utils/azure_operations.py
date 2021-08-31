from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.models import Variable
import pandas as pd

azure = WasbHook(wasb_conn_id="azure_blob")
container = Variable.get("env_azure_container")
blob = Variable.get("env_input_blob")
env_file_path = Variable.get("env_file_path")
v_batch_ingestion_blob_output_path = Variable.get("v_batch_ingestion_blob_output_path")


def azure_connection():
    return azure.check_for_blob(container, blob)


def convert_json_to_csv():
    # TODO: making it into chunks if need be
    # file = azure.download(container, blob,0)
    # file._config.max_chunk_get_size = 1000
    # for chunk in file.chunks():
    #     print("chunk", chunk)
    file = azure.read_file(container, blob, max_concurrency=5)
    dataframe = pd.read_json(file)
    dataframe.to_csv(env_file_path)


def convert_parquet_to_csv():
    file = azure.read_file(container, blob, max_concurrency=5)
    dataframe = pd.read_parquet(file)
    dataframe.to_csv(env_file_path)


def read_azure_blob_file():
    file_ext = blob.split(".")[-1].upper()
    if file_ext == "CSV":
        azure.get_file(env_file_path, container, blob)
    elif file_ext == "JSON":
        convert_json_to_csv()
    elif file_ext == "PARQUET":
        convert_parquet_to_csv()


def write_to_azure_blob():
    azure.load_file(env_file_path, container, v_batch_ingestion_blob_output_path)

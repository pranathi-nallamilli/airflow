from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.models import Variable
import pandas as pd
from airflow.models import Connection
from airflow import settings
import logging
import os
import io
from azure.storage.blob import BlobServiceClient
from airflow import AirflowException

AZURE_CONN_ID = Variable.get("env_azure_connection")
AZURE_CONN_TYPE = Variable.get("env_azure_connection_type")
AZURE_ACCOUNT = Variable.get("env_azure_account")
ACCOUNT_PWD = Variable.get("env_azure_pwd")
container = Variable.get("env_azure_container")
env_file_path = Variable.get("env_file_path")
v_batch_ingestion_blob_output_path = Variable.get("v_batch_ingestion_blob_output_path")


def get_blob_list(path, recursive=False):
    def connection_string(account, key):
        return (
            "DefaultEndpointsProtocol=https;AccountName="
            + account
            + ";AccountKey="
            + str(key)
            + ";EndpointSuffix=core.windows.net"
        )

    def get_valid_blobs(blobs):
        files = []
        for blob in blobs:
            if blob.split(".")[-1].upper() in ["CSV","JSON","PARQUET"]:
                files.append(blob)
        Variable.set("v_container_files", files)

    service_client = BlobServiceClient.from_connection_string(
        conn_str=connection_string(AZURE_ACCOUNT, ACCOUNT_PWD)
    )
    client = service_client.get_container_client(container)

    if not path == "" and not path.endswith("/"):
        path += "/"

    blob_iter = client.list_blobs(name_starts_with=path)
    files = []
    for blob in blob_iter:
        print(blob.name)
        relative_path = os.path.relpath(blob.name, path)
        if recursive or not "/" in relative_path:
            files.append(relative_path)

    print(files)
    get_valid_blobs(files)    


def create_connection_azure():
    conn = Connection(
        conn_id=AZURE_CONN_ID,
        conn_type=AZURE_CONN_TYPE,
        login=AZURE_ACCOUNT,
        password=ACCOUNT_PWD,
    )
    session = settings.Session()
    conn_name = (
        session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()
    )

    if str(conn_name) == str(AZURE_CONN_ID):
        return logging.info(f"Connection {AZURE_CONN_ID} already exists")
    session.add(conn)
    session.commit()
    logging.info(Connection.log_info(conn))
    logging.info(f"Connection {AZURE_CONN_ID} is created")


def read_azure_blob_file(blob):
    azure = WasbHook(wasb_conn_id=AZURE_CONN_ID)
    # list = blob.lstrip('[').rstrip(']').split(',')
    # for item in list:
    #     item=item.strip().lstrip('\'').rstrip('\'')
    #     print(item)
    # print(blob[0])

    # TODO: making it into chunks if need be
    # file = azure.download(container, blob,0)
    # file._config.max_chunk_get_size = 1000
    # for chunk in file.chunks():
    #     print("chunk", chunk)

    print("Downloading blob", blob)
    
    file_ext = blob.split(".")[-1].upper()
    print("File extension is", blob)
    file = azure.read_file(container, blob, max_concurrency=5)
    print("file contents", file)

    if file_ext == "CSV":
        dataframe = pd.read_csv(io.StringIO(file))
    elif file_ext == "JSON":
        dataframe = pd.read_json(file)
    elif file_ext == "PARQUET":
        dataframe = pd.read_parquet(file)
    else:
        raise AirflowException(
            f"Blob file has invalid extension!!\nFilename :{blob}"
        )

    dataframe.to_csv(env_file_path, index=False)


def write_to_azure_blob():
    azure = WasbHook(wasb_conn_id=AZURE_CONN_ID)
    azure.load_file(env_file_path, container, v_batch_ingestion_blob_output_path)

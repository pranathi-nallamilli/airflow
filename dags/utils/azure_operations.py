from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.models import Variable
import pandas as pd
import io

azure = WasbHook(wasb_conn_id='azure_blob')
container = Variable.get('env_azure_container')
blob = Variable.get('env_input_blob')
env_file_path  =Variable.get('env_file_path')

def azure_connection():
 return(azure.check_for_blob(container,blob))

def read_azure_blob_file():
    file = azure.read_file(container,blob)
    print(file)
    #write file locally
    df = pd.read_csv(io.StringIO(file)  , sep=",")
    print(df)
    df.to_csv(env_file_path, index=False)

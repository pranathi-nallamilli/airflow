import sys

sys.path.insert(0, "/opt/airflow/dags/utils/")

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from utils import azure_operations as az
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from utils import mapping_and_validation as mv
from utils import get_schema as gc
from utils import data_catalog_lookup as dc
from utils import snowflake_db
from utils import read_ds_dt
from utils import file_manifest_prep as fp
from utils import derive_value_and_validations as dvv
from utils import get_iteration_number as gi

default_args = {
    "start_date": datetime(2020, 1, 1),
    "owner": "Airflow",
    "email": "owner@test.com",
    "schedule_interval": "@daily",
}
with DAG(dag_id="combined_dag", default_args=default_args, catchup=False) as dag:

    create_azure_connection = PythonOperator(
        task_id="create_azure_connection",
        python_callable=az.create_connection_azure,
        dag=dag,
    )

    get_all_blobs = PythonOperator(
        task_id="get_all_blobs",
        python_callable=az.get_blob_list,
        op_kwargs={"path": "", "recursive": True},
        dag=dag,
    )

    with TaskGroup("data_prep_tasks") as data_prep_tasks:
        blobs = eval(Variable.get("v_container_files"))
        for index, blob in enumerate(blobs):
            blob_index = f"_{index}"
            print("Data prep started for file", blob)
            
            read_blob_file = PythonOperator(
                task_id="read_blob_file" + blob_index,
                python_callable=az.read_azure_blob_file,
                op_kwargs={"blob": blob},
                dag=dag,
            )

            set_iteration_variables = PythonOperator(
                task_id="set_iteration_variables" + blob_index,
                python_callable=gi.set_iteration_variables,
                dag=dag,
            )

            manifest_file = PythonOperator(
                task_id="manifest_file" + blob_index,
                python_callable=fp.check_file_path,
                dag=dag,
            )

            validate_file = PythonOperator(
                task_id="validate_file" + blob_index,
                python_callable=dvv.derive_and_validate_file,
                dag=dag,
            )

            check_snowflake_connection = PythonOperator(
                task_id="check_snowflake_connection" + blob_index,
                python_callable=snowflake_db.create_snowflake_connection,
                dag=dag,
            )

            get_schema = PythonOperator(
                task_id="get_schema" + blob_index,
                python_callable=gc.initiate_get_schema,
                dag=dag,
            )

            read_DS_DT = PythonOperator(
                task_id="read_DS_DT_task" + blob_index,
                python_callable=read_ds_dt.read_DS_DT_operation,
                dag=dag,
            )

            data_catalog_lookup_task = PythonOperator(
                task_id="data_catalog_lookup" + blob_index,
                python_callable=dc.lookup_data_catalog,
                dag=dag,
            )

            mapping_and_validation = PythonOperator(
                task_id="mapping_and_validation" + blob_index,
                python_callable=mv.mapping_validation_initiate_operation,
                dag=dag,
            )

            (
                read_blob_file
                >> set_iteration_variables
                >> manifest_file
                >> validate_file
                >> check_snowflake_connection
                >> get_schema
                >> read_DS_DT
                >> data_catalog_lookup_task
                >> mapping_and_validation
            )

    create_azure_connection >> get_all_blobs >> data_prep_tasks

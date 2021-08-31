from airflow.models import Variable

# SQL commands
map_query = f"SELECT FIELD_NAME, FIELD_VARIATION FROM {Variable.get('env_admin_database')}.{Variable.get('env_admin_schema')}.CDP_FIELD_NAME_VARIATIONS WHERE FEED_ID = {Variable.get('v_feed_id')}"
mandatory_columns_query = f"SELECT REQUIRED_FIELDS FROM {Variable.get('env_admin_database')}.{Variable.get('env_admin_schema')}.CDP_REQUIRED_FIELDS WHERE PIPELINE = '{Variable.get('v_pipeline')}'"

# Data Catalog Lookup query to determine the nature of the information (PII or not) in the file
pii_query = f"SELECT IFNULL(UPPER(DFC.PIPELINE), 'NULL') AS PIPELINE, IFNULL(LOWER(CP.MICROSERVICE_LIST),'') PIPELINE_FLOW,  UPPER(DFC.FEED_ID) FEED_ID " 
pii_query += f"FROM {Variable.get('env_admin_database')}.{Variable.get('env_admin_schema')}.{Variable.get('env_data_feed_catalog')} DFC "
pii_query += f"INNER JOIN {Variable.get('env_admin_database')}.{Variable.get('env_admin_schema')}.{Variable.get('env_cdp_pipeline_table')} CP ON UPPER(DFC.PIPELINE) = UPPER(CP.PIPELINE) "
pii_query += f"WHERE UPPER(DFC.DATA_SOURCE) = '{Variable.get('v_data_source').upper()}' AND UPPER(DFC.DATA_TYPE) = '{Variable.get('v_data_type').upper()}' LIMIT 1"

create_iteration_number_sequence_query = 'IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.SEQUENCES WHERE SEQUENCE_NAME = "cdp_ingestion_seq") BEGIN CREATE SEQUENCE cdp_ingestion_seq START WITH 1 INCREMENT BY 1 NOCYCLE ; END'
iteration_number_query = 'select ETL_STAGING.HXP_MATILLION_STG.cdp_ingestion_seq.nextval ITER_NUM'
run_history_id = Variable.get("run_history_id")
v_iter_num = Variable.get("v_iter_num")
v_unq_id = Variable.get("v_unq_id")

v_sub_folder = Variable.get("v_sub_folder")
v_error_flag = Variable.get("v_error_flag")
v_filename = Variable.get("v_filename")
v_base_folder = Variable.get("v_base_folder")

v_data_source_flag = Variable.get("v_data_source_flag")
v_data_type_flag = Variable.get("v_data_type_flag")
v_data_source = Variable.get("v_data_source")
v_data_type = Variable.get("v_data_type")
v_field_seperator = Variable.get('v_field_seperator')
env_file_path=Variable.get('env_file_path')

v_batch_ingestion_path = Variable.get('v_batch_ingestion_path')
v_batch_ingestion_file_path = Variable.get('v_batch_ingestion_file_path')
env_invalid_chars_path = Variable.get('env_invalid_chars_path')
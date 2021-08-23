from airflow.models import Variable

# SQL commands
map_query = f"SELECT FIELD_NAME, FIELD_VARIATION FROM {Variable.get('v_admin_database')}.{Variable.get('v_admin_schema')}.CDP_FIELD_NAME_VARIATIONS WHERE FEED_ID = {Variable.get('v_feed_id')}"
mandatory_columns_query = f"SELECT REQUIRED_FIELDS FROM {Variable.get('v_admin_database')}.{Variable.get('v_admin_schema')}.CDP_REQUIRED_FIELDS WHERE PIPELINE = '{Variable.get('v_pipeline')}'"

# Data Catalog Lookup query to determine the nature of the information (PII or not) in the file
pii_query = f"SELECT IFNULL(UPPER(DFC.PIPELINE), 'NULL') AS PIPELINE, IFNULL(LOWER(CP.MICROSERVICE_LIST),'') PIPELINE_FLOW,  UPPER(DFC.FEED_ID) FEED_ID " 
pii_query += f"FROM {Variable.get('v_admin_database')}.{Variable.get('v_admin_schema')}.{Variable.get('v_data_feed_catalog')} DFC "
pii_query += f"INNER JOIN {Variable.get('v_admin_database')}.{Variable.get('v_admin_schema')}.{Variable.get('v_cdp_pipeline_table')} CP ON UPPER(DFC.PIPELINE) = UPPER(CP.PIPELINE) "
pii_query += f"WHERE UPPER(DFC.DATA_SOURCE) = '{Variable.get('v_data_source').upper()}' AND UPPER(DFC.DATA_TYPE) = '{Variable.get('v_data_type').upper()}' LIMIT 1"


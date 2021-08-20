from airflow.models import Variable

# SQL commands
map_query1 = f"SELECT FIELD_NAME, FIELD_VARIATION FROM {Variable.get('v_admin_database')}.{Variable.get('v_admin_schema')}.CDP_FIELD_NAME_VARIATIONS WHERE FEED_ID = {Variable.get('v_feed_id')}"
map_query2 = f"SELECT FIELD_NAME, FIELD_VARIATION FROM {Variable.get('v_admin_database')}.{Variable.get('v_admin_schema')}.CDP_FIELD_NAME_VARIATIONS WHERE PIPELINE = '{Variable.get('v_pipeline')}'"     
mandatory_columns_query1 = f"SELECT REQUIRED_FIELDS FROM {Variable.get('v_admin_database')}.{Variable.get('v_admin_schema')}.CDP_REQUIRED_FIELDS WHERE FEED_ID = {Variable.get('v_feed_id')}"
mandatory_columns_query2 = f"SELECT REQUIRED_FIELDS FROM {Variable.get('v_admin_database')}.{Variable.get('v_admin_schema')}.CDP_REQUIRED_FIELDS WHERE PIPELINE = '{Variable.get('v_pipeline')}'"

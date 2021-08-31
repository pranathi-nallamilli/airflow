# from datetime import datetime
# from airflow.models import Variable
# import sys
# sys.path.insert(0, '/opt/airflow/dags/utils/')
# from utils import snowflake_db
# import pandas as pd

# file_path = Variable.get('v_batch_ingestion_stage') + Variable.get('v_batch_ingestion_stage_file_path')
# #v_file_path = Variable.get('v_file_path')
# v_file_ext = Variable.get('v_file_ext')
# v_field_seperator = Variable.get('v_field_seperator')
# v_schema_file = Variable.get('v_schema_file')
# src_fields = Variable.get('v_src_fields')
# view_name = Variable.get('v_batch_ingestion_out_view')
# v_data_source = Variable.get('v_data_source')
# v_data_type = Variable.get('v_data_type')
# v_filename=  'airflow_file_processing_demo'

# df = pd.read_csv(v_schema_file, delimiter=',')
# schema = [list(row) for row in df.values]
# file_format = 'CSV_DATA_FORMAT'
# ds_flag  = 'Y'
# dt_flag = 'N'

# def create_view_for_csv():
#   pos = 0
#   view_query='CREATE OR REPLACE VIEW "'+view_name+'" AS (SELECT '
    
#   headers = []
#   for col in schema:
#     #pos = pos+1
#     #view_query = view_query + "$"+str(pos) + ' as "' + str(col[1]) +'", '
#     headers.append(col[1])

#   if ds_flag.upper() == 'N':
#     #view_query = view_query+ "'"+str(v_data_source.upper())+"' as "+str('DATA_SOURCE')+", "
#     headers.append(['DATA_SOURCE'])
#   if dt_flag.upper() == 'N':
#     #view_query = view_query+ "'"+str(v_data_type.upper()) + "' as " +str('DATA_TYPE')+", "
#     headers.append(['DATA_TYPE'])
    
  # view_query = view_query+"metadata$file_row_number as SEQ, "
  # headers.append(['SEQ','SEQ'])
  # view_query = view_query+ "'"+v_filename+"' as FILENAME_IN, "
  # headers.append(['FILENAME_IN','FILENAME_IN'])
  # view_query = view_query+"CONCAT("+str(run_history_id)+",LPAD("+v_iter_num+", 4, '0')) as UNQ_ID, "
  # headers.append(['UNQ_ID','UNQ_ID'])
  
  # headers.sort(key=lambda x: x[0])
  # exception_list = context.getGridVariable('field_exclusion_for_hash')
  # view_query = view_query+ "MD5(CONCAT("+fields_for_data_hash(headers,exception_list)+")) as DATA_HASH, "
  
  # view_query = view_query[0:len(view_query)-2]
  # view_query = view_query+ " FROM '@"+file_path+"'(file_format => "+file_format+") t);"
  
  # print(view_query)
  #output = snowflake_db.execute_snowflake_fetchone()


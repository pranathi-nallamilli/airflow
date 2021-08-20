from datetime import datetime
from airflow.models import Variable
from contextlib import closing
import sys
sys.path.insert(0, '/opt/airflow/dags/utils/')
from utils import snowflake_db
from utils import config_data
from csv import reader
import pandas as pd

v_file_path = Variable.get('v_batch_ingestion_stage')+Variable.get('v_batch_ingestion_stage_file_path')
v_file_ext = Variable.get('v_file_ext')
v_field_seperator = Variable.get('v_field_seperator')
v_flat_file_header_format = Variable.get('v_flat_file_header_format')

def get_csv_schema(file_path, file_format):
  schema_query="SELECT top 1 replace($1,'\"') header FROM '@"+file_path+"'(file_format => "+file_format+") t;"
  print(schema_query)

  try:
    header = snowflake_db.execute_snowflake_fetchall(schema_query)[0]

  except Exception as e:
    raise Exception("Input file schema is not correct: "+str(e))
  
  df = pd.read_csv('/opt/airflow/dags/utils/src_fields.csv', delimiter=',')
  v_src_fields = [list(row) for row in df.values]

  seen = {}
  cols = []
  duplicates = []
  headers_list = header.split(v_field_seperator)

  for i in range(len(headers_list)):
    if(headers_list[i] is None or headers_list[i] == ''):
      raise Exception("Input file has one or more blank or empty field names")
    renamed = False
    for field in v_src_fields:
      if(field[0].upper() == headers_list[i].upper()):
        cols.append([headers_list[i], 'SRC_'+field[0].upper()])
        renamed = True
        break
    
    if(not renamed):
      cols.append([headers_list[i], headers_list[i].upper()])

    if (headers_list[i].upper() not in seen.keys()):
      seen[headers_list[i].upper()] = i
    else:
      duplicates.append(seen[headers_list[i].upper()])
      duplicates.append(headers_list[i])
  
  if(len(duplicates)>0):
    raise Exception("Input file has multiple fields with same name. Duplicate fields :" + str(duplicates))
    
  return cols


def initiate_get_schema(**kwargs):
    if(v_file_ext.upper() == 'CSV'):
        v_schema = get_csv_schema(v_file_path, v_flat_file_header_format)
    else:
        v_schema = get_json_or_parquet_schema(v_file_path, v_file_format)

    context.updateGridVariable('schema', v_schema)
    print(v_file_ext + "Headers: "+ v_schema)

    for ele in v_schema:
        if('DATA_SOURCE' in ele[0].upper()):
            Variable.set('v_data_source_flag','Y')
            print("Data Source Field Found in file")
        if('DATA_TYPE' in ele[0].upper()):
            Variable.set('v_data_type_flag','Y')
            print("Data Type Field Found in file")
        if('SEQ' in ele[0].upper()):
            Variable.set('v_seq_present', 'Y')
            print ('SEQ field found!')



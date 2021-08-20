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
v_file_format = Variable.get('v_file_format')

def get_json_or_parquet_schema(file_path, file_format):
  schema_query="SELECT DISTINCT f.path COL_NAME  FROM '@"+file_path+"'(file_format => "+file_format+") t ,LATERAL FLATTEN($1, RECURSIVE=>true) f WHERE TYPEOF(f.value) != 'OBJECT';"
  print(schema_query)

  try:
    headers_list = snowflake_db.execute_snowflake_fetchall(schema_query)

  except Exception as e:
    raise Exception("Input file schema is not correct: "+str(e))
  
  df = pd.read_csv('/opt/airflow/dags/utils/src_fields.csv', delimiter=',')
  v_src_fields = [list(row) for row in df.values]
  cols=[]

  for i in range(len(headers_list)):
    headers_list[i] = str(headers_list[i][0]).replace("['",'').replace("']",'')

  cols = parseSchema(v_src_fields,headers_list[i])

  return cols

def parseSchema(v_src_fields,headers_list):
    seen = {}
    cols = []
    duplicates = []
  
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

    for ele in cols:
        if('DATA_SOURCE' in ele[0].upper()):
            Variable.set('v_data_source_flag','Y')
            print("Data Source Field Found in file")
        if('DATA_TYPE' in ele[0].upper()):
            Variable.set('v_data_type_flag','Y')
            print("Data Type Field Found in file")
        if('SEQ' in ele[0].upper()):
            Variable.set('v_seq_present', 'Y')
            print ('SEQ field found!')

    return cols
    
def get_csv_schema(file_path, file_format):
  schema_query="SELECT top 1 replace($1,'\"') header FROM '@"+file_path+"'(file_format => FLAT_FILE_HEADER_FORMAT) t;"
  print(schema_query)

  try:
    header = snowflake_db.execute_snowflake_fetchone(schema_query)[0]

  except Exception as e:
    raise Exception("Input file schema is not correct: "+str(e))
  
  df = pd.read_csv('/opt/airflow/dags/utils/src_fields.csv', delimiter=',')
  v_src_fields = [list(row) for row in df.values]

  cols = []
  headers_list = header.split(v_field_seperator)
  parseSchema(v_src_fields,headers_list)
  return cols

def initiate_get_schema(**kwargs):
    if(v_file_ext.upper() == 'CSV'):
        v_schema = get_csv_schema(v_file_path, v_flat_file_header_format)
    else:
        v_schema = get_json_or_parquet_schema(v_file_path, v_file_format)

    #write back to schema
    df2 = pd.DataFrame(v_schema)
    df2.to_csv('/opt/airflow/dags/utils/schema.csv', index=False)
    
    print(v_file_ext + "Headers: "+ v_schema)

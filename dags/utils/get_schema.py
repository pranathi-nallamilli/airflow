from datetime import datetime
from airflow.models import Variable
import sys
sys.path.insert(0, '/opt/airflow/dags/utils/')
from utils import snowflake_db
import pandas as pd

env_file_path = Variable.get('env_file_path')
v_file_ext = Variable.get('v_file_ext')
v_field_seperator = Variable.get('v_field_seperator')
src_fields = Variable.get('env_src_fields')

def parseHeaders(headers_list):
    seen = {}
    cols = []
    duplicates = []

    df = pd.read_csv(src_fields, delimiter=',')
    v_src_fields = [list(row) for row in df.values]

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

    print(cols)
    df=pd.read_csv(env_file_path,delimiter=v_field_seperator)
    df.to_csv(env_file_path,header=[ele[1] for ele in cols],index=False)
    

def initiate_get_schema(**kwargs):
    headers_list = []
    try:
      if(v_file_ext.upper() == 'CSV'):
          headers_list = pd.read_csv(env_file_path, delimiter=v_field_seperator).columns
      elif(v_file_ext.upper() == 'JSON'):
          headers_list = pd.read_csv(env_file_path, delimiter=v_field_seperator).columns
      else:
          headers_list = pd.read_parquet(env_file_path).columns
      print(headers_list)
    except Exception as e:
      raise Exception("Input file schema is not correct: "+ e)

    parseHeaders(headers_list)


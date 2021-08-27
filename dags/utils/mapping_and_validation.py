from datetime import datetime
from airflow.models import Variable
from contextlib import closing
import sys
sys.path.insert(0, '/opt/airflow/dags/utils/')
from utils import snowflake_db
from utils import config_data
from csv import reader
import pandas as pd

env_file_path = Variable.get('env_file_path')
v_field_seperator = Variable.get('v_field_seperator')
v_data_source_flag = Variable.get('v_data_source_flag')
v_data_type_flag = Variable.get('v_data_type_flag')

def mapping_validation_initiate_operation(**kwargs): 

    map_result = snowflake_db.execute_snowflake_fetchall(config_data.map_query)
    
    cols = UpdateMapping(map_result)
    processRequiredFields(cols)

def processRequiredFields(cols):
    # executing query
    try:
        req_result = snowflake_db.execute_snowflake_fetchall(config_data.mandatory_columns_query)
        mandatory_columns_arr = [ele[0] for ele in req_result]
    
    except Exception as e:
        raise Exception("Validation Table Query Failed with error: "+ e)

    # convert exception
    if('FULLNAME' in cols):
        cols.append('FNAME')
        cols.append('LNAME')

    missed_req_cols = list(set(mandatory_columns_arr)-set(cols))

    if(len(missed_req_cols)>0):
        raise Exception("Required Columns missing: " + missed_req_cols)

def UpdateMapping(map_result):
    
    v_map_cols = [list(ele) for ele in map_result]
    cols= []
    df = pd.read_csv(env_file_path, delimiter=v_field_seperator)
    cols=df.columns
    print(cols)
    print(v_map_cols)

    for i in range(len(cols)):
        for maplist in v_map_cols:
            if(cols[i].upper() == maplist[1].upper()):
                print ('Mapping field [' +cols[i]+ '] to [' +maplist[0].upper()+ ']')
                cols[i] = maplist[0].upper()

    seen = []
    duplicates = [] 

    for col in cols:
        if(col.upper() not in seen):
            seen.append(col)
        else:
            duplicates.append(col)
            break
    print(seen)
    print(duplicates)

    if(len(duplicates)>0):
        raise Exception("Multiple fields are getting mapped to same field, creating duplicate fields. Duplicate fields :" + str(duplicates))
    
    df.to_csv(env_file_path,header=cols,index=False)
    return cols
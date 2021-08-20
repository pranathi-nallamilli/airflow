from datetime import datetime
from airflow.models import Variable
from contextlib import closing
import sys
sys.path.insert(0, '/opt/airflow/dags/utils/')
from utils import snowflake_db
from utils import config_data
from csv import reader
import pandas as pd


def snowflake_data_operation(**kwargs): 

    map_result = snowflake_db.execute_snowflake_fetchall(config_data.map_query1)
    if len(map_result)==0:
                print('Feed specifc field mapping configuration not found for feed id: '+ Variable.get('v_feed_id'))
                map_result = snowflake_db.execute_snowflake_fetchall(config_data.map_query2)

    cols = processMappingAndUpdateSchema(map_result)
    processRequiredFields(cols)

def processRequiredFields(cols):
    # executing query
    try:
        req_result = snowflake_db.execute_snowflake_fetchall(config_data.mandatory_columns_query1)

        if len(req_result)==0:
            print('Feed specifc required field configuration not found for feed id: '+ Variable.get('v_feed_id'))
            req_result = snowflake_db.execute_snowflake_fetchall(config_data.mandatory_columns_query2)
        mandatory_columns_arr = [ele[0] for ele in req_result]
    
    except Exception as e:
        raise Exception("Validation Table Query Failed with error: "+e)

    # convert exception
    cols = [ele[1] for ele in cols]
    if('FULLNAME' in cols):
        cols.append('FNAME')
        cols.append('LNAME')

    missed_req_cols = list(set(mandatory_columns_arr)-set(cols))

    if(len(missed_req_cols)>0):
        raise Exception("Required Columns missing: " + missed_req_cols)

def processMappingAndUpdateSchema(map_result):
    
    v_map_cols = [list(ele) for ele in map_result]

    cols= []
    df = pd.read_csv('/opt/airflow/dags/utils/schema.csv', delimiter=',')
    cols = [list(row) for row in df.values]
    print(cols)
    
    print(v_map_cols)
    for i in range(len(cols)):
        for maplist in v_map_cols:
            if(cols[i][0].upper() == maplist[1].upper()):
                print ('Mapping field [' +cols[i][0]+ '] to [' +maplist[0].upper()+ ']')
                cols[i][1] = maplist[0].upper()

    seen = {}
    duplicates = [] 

    for col in cols:
        if(col[1].upper() not in seen.keys()):
            seen[col[1]] = col[0]
        else:
            duplicates.append([seen[col[1]],col[1]])
            duplicates.append([col[0],col[1]])     
    print(seen)
    print(duplicates)

    if(len(duplicates)>0):
        raise Exception("Multiple fields are getting mapped to same field, creating duplicate fields. Duplicate fields :" + str(duplicates))

    #write back to schema
    df2 = pd.DataFrame(cols, columns=df.columns)
    df2.to_csv('/opt/airflow/dags/utils/schema.csv', index=False)
    return cols
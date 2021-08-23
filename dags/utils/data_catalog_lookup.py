from airflow.models import Variable
import sys
sys.path.insert(0, '/opt/airflow/dags/utils/')
from utils import snowflake_db
from utils import config_data

def lookup_data_catalog():
# executing query
    try:
        print(config_data.pii_query)
        pipeline_status = snowflake_db.execute_snowflake_fetchall(config_data.pii_query)
    except Exception as e:
        raise Exception("Data Catalog Query Failed with error: "+str(e))

# if no match found for the data source and data type combination in the Data Catalog table, then it is new-feed
    if(len(pipeline_status) == 0):
        pipeline = "NULL"
    else:
        pipeline = str(pipeline_status[0][0]).upper()
        Variable.set('v_pipeline_flow',str(pipeline_status[0][1]).lower())
        Variable.set('v_feed_id',str(pipeline_status[0][2]).upper()) 

    Variable.set('v_pipeline', pipeline)
    commonPath= Variable.get('v_data_source').upper()+'/'+Variable.get('v_data_type').upper()+'/'+Variable.get('v_year_string')+'/'+Variable.get('v_month_string')
  
    if(Variable.get('v_pipeline') == 'NULL'):
        print ("The input file contains a new-feed or pipeline configuration doesn't exists.")
        Variable.set('v_new_feed_flag','Y')
        Variable.set('v_is_microservice_processing','NULL')
        batch_ingestion_archived_path = Variable.get('v_data_catalog_newfeed_blob')+commonPath
        Variable.set('v_batch_ingestion_archive_newfeed_path',batch_ingestion_archived_path)
        
        print(f"The file {Variable.get('v_batch_ingestion_filename')}.{Variable.get('v_file_ext')} will be moved to blob location"+batch_ingestion_archived_path)

    else:
        print ('The input file contains '+pipeline+' feed.')

        if(Variable.get('v_pipeline_flow')!=''):
            Variable.set('v_is_microservice_processing','Y')
            v_pipeline_flow_msg = list(Variable.get('v_pipeline_flow').lower().split(','))
            Variable.set('v_output_folder',v_pipeline_flow_msg[0])

        print("The Ingestion Pipeline flow will be "+ Variable.get('v_pipeline_flow'))
        print("The Ingestion feed id is "+Variable.get('v_feed_id'))

    v_file_folder = Variable.get('v_batch_ingestion_filename') + '/'

    # This dictionary will be used to assign the output blob path depending on the Feed type (PII, non-PII, new-feed)
    pii_location_switch = {
        "N" : f"{Variable.get('v_landing_path')}"+ commonPath+'/'+v_file_folder,
        "Y" : f"{Variable.get('v_microservices_processing_path')}"+ commonPath+'/'+v_file_folder,
        "NULL" : f"{Variable.get('v_data_catalog_newfeed_blob')}"
    }

    # send the file to respective container depending on the data feed catalog lookup
    Variable.set('v_batch_ingestion_blob_output_path', pii_location_switch.get(Variable.get('v_is_microservice_processing'), Variable.get('v_data_catalog_error_path')))

    print(f"The output file {Variable.get('v_batch_ingestion_filename')} will be written to blob location {Variable.get('v_batch_ingestion_blob_output_path')}")

from airflow.models import Variable
import sys
sys.path.insert(0, '/opt/airflow/dags/utils/')
from utils import snowflake_db
from config_data import (
    run_history_id,
    v_iter_num,
    v_unq_id,
    iteration_number_query,
    create_iteration_number_sequence_query
)

# Set Iteration Number and Unique ID
def set_iteration_variables():
    # snowflake_db.execute_snowflake_fetchone(create_iteration_number_sequence_query)
    iteration_number = snowflake_db.execute_snowflake_fetchone(iteration_number_query)[0]
    Variable.set("v_iter_num", iteration_number)
    Variable.set("v_unq_id", f"{str(run_history_id)}_{str(v_iter_num)}")

    print(f"Iteration number: {str(v_iter_num)}")
    print("Unique Id:" + str(v_unq_id))
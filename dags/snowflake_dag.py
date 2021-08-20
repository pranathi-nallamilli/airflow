#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Example use of Snowflake related operators.
"""
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.providers.snowflake.transfers.snowflake_to_slack import SnowflakeToSlackOperator
from airflow.utils.dates import days_ago

SNOWFLAKE_CONN_ID = 'snowflake_connection'
SLACK_CONN_ID = 'my_slack_conn'
# TODO: should be able to rely on connection's schema, but currently param required by S3ToSnowflakeTransfer
SNOWFLAKE_SCHEMA = 'schema_name'
SNOWFLAKE_STAGE = 'stage_name'
SNOWFLAKE_WAREHOUSE = 'warehouse_name'
SNOWFLAKE_DATABASE = 'database_name'
SNOWFLAKE_ROLE = 'role_name'
SNOWFLAKE_SAMPLE_TABLE = 'sample_table2'
S3_FILE_PATH = '</path/to/file/sample_file.csv'

# SQL commands
CREATE_TABLE_SQL_STRING = (
    f"CREATE OR REPLACE TRANSIENT TABLE {SNOWFLAKE_SAMPLE_TABLE} (name VARCHAR(250), id INT);"
)
SQL_INSERT_STATEMENT = f"INSERT INTO {SNOWFLAKE_SAMPLE_TABLE} VALUES ('name', %(id)s)"
SQL_LIST = [SQL_INSERT_STATEMENT % {"id": n} for n in range(0, 10)]
SQL_MULTIPLE_STMTS = "; ".join(SQL_LIST)
SNOWFLAKE_SLACK_SQL = f"SELECT name, id FROM {SNOWFLAKE_SAMPLE_TABLE} LIMIT 10;"
SNOWFLAKE_SLACK_MESSAGE = (
    "Results in an ASCII table:\n```{{ results_df | tabulate(tablefmt='pretty', headers='keys') }}```"
)

with DAG(dag_id='example_snowflake', schedule_interval='@daily', start_date=days_ago(2), catchup=False, tags=['custom']) as dag:

# [START howto_operator_snowflake]

    snowflake_op_sql_str = SnowflakeOperator(
        task_id='snowflake_op_sql_create',
        dag=dag,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=CREATE_TABLE_SQL_STRING,
    )

    task_insert = SnowflakeOperator(
        task_id='snowflake_op_sql_insert',
        dag=dag,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=SQL_MULTIPLE_STMTS,
    )

    task_select = SnowflakeOperator(
        task_id='snowflake_op_sql_select',
        dag=dag,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=SNOWFLAKE_SLACK_SQL,
    )

    snowflake_op_sql_str >> task_insert >> task_select

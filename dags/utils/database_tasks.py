"""File for database connection or querying related scripts"""
import logging
from contextlib import closing
import utils.file_operations as fo
import pandas as pd

from airflow import settings
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.models import Connection, Variable
from airflow.providers.mongo.hooks.mongo import MongoHook

SELECTED_DB = Variable.get("env_selected_db")
SELECTED_QUERY = Variable.get("env_selected_query")

SNOWFLAKE_CONN_ID = Variable.get("env_snowflake_connection")
SNOWFLAKE_CONN_TYPE = Variable.get("env_connection_type")
SNOWFLAKE_HOST = Variable.get("env_snowflake_host")
SNOWFLAKE_LOGIN = Variable.get("env_snowflake_login")
SNOWFLAKE_PWD = Variable.get("env_snowflake_password")
SNOWFLAKE_SCHEMA = Variable.get("env_admin_schema")
SNOWFLAKE_EXTRA = Variable.get("env_snowflake_extra")

MONGO_CONN_ID = Variable.get("env_mongo_connection")
MONGO_CONN_TYPE = Variable.get("env_mongo_connection_type")
MONGO_HOST = Variable.get("env_mongo_host")
MONGO_DB = Variable.get("env_mongo_database")
MONGO_COLLECTION = Variable.get("env_mongo_collection")

QUERY_0 = "select a.pipeline, a.pipeline_id, a.microservice_list" + \
    "from 'CDP_ADMIN_5_0'.'CTRL'.'CDP_PIPELINE' as a"

QUERY_1 = "select b.id, b.feed_id, b.data_source, b.data_type, b.feed_type,b.pipeline" + \
    "from 'CDP_ADMIN_5_0'.'CTRL'.'DATA_FEED_CATALOG' as b"

QUERY_2 = "select c.id, c.required_fields,c.pipeline" + \
    "from 'CDP_ADMIN_5_0'.'CTRL'.'CDP_REQUIRED_FIELDS' as c"

QUERY_3 = "select d.id, d.field_variation, d.field_name, d.pipeline" + \
    "from 'CDP_ADMIN_5_0'.'CTRL'.'CDP_FIELD_NAME_VARIATIONS' as d"

CONFIG_PATH = f"{Variable.get('env_data_path')}config/config_data.json"


def create_database_connection():
    """Method to create a database connection"""
    conn = None
    conn_id = None
    if SELECTED_DB == "SNOWFLAKE":
        conn = Connection(
            conn_id=SNOWFLAKE_CONN_ID,
            conn_type=SNOWFLAKE_CONN_TYPE,
            host=SNOWFLAKE_HOST,
            login=SNOWFLAKE_LOGIN,
            password=SNOWFLAKE_PWD,
            schema=SNOWFLAKE_SCHEMA,
            extra=SNOWFLAKE_EXTRA,
        )
        conn_id = SNOWFLAKE_CONN_ID

    elif SELECTED_DB == "MONGODB":
        conn = Connection(
            conn_id=MONGO_CONN_ID, conn_type=MONGO_CONN_TYPE, host=MONGO_HOST
        )
        conn_id = MONGO_CONN_ID

    session = settings.Session()
    conn_name = (
        session.query(Connection)
        .filter(Connection.conn_id == conn.conn_id)
        .first()
    )
    logging.info("Connection_name %s", conn_name)
    logging.info("Connection_ID %s", conn_id)
    if str(conn_name) == str(conn_id):
        logging.info("Connection %s already exists", conn_id)
    else:
        session.add(conn)
        session.commit()
        logging.info(Connection.log_info(conn))
        logging.info("Connection %s is created", conn_id)


def get_db_hook():
    """Method to get the respective Database Hook to perform queries from connection"""
    if SELECTED_DB == "SNOWFLAKE":
        return SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

    if SELECTED_DB == "MONGODB":
        return MongoHook(conn_id=MONGO_CONN_ID)

    return None


def execute_fetchall(query=None, with_cursor=False):
    """Method to execute a fetchall query"""
    if SELECTED_QUERY == "SQL":
        with closing(get_db_hook().get_conn()) as conn:
            with closing(conn.cursor()) as cur:
                cur.execute(query)
                result = cur.fetchall()
                if with_cursor:
                    return (result, cur)
                return result
    else:
        return list(
            get_db_hook().find(
                query=query,
                find_one=False,
                mongo_collection=MONGO_COLLECTION,
                mongo_db=MONGO_DB,
            )
        )


def setup_config_data():
    """Method to query the Database and create the config_data.json file"""

    if SELECTED_QUERY == "SQL":
        arr_test = []
        df_pipeline = pd.DataFrame(
            execute_fetchall(QUERY_0),
            columns=("pipeline", "pipeline_id", "microservice_list"),
        )

        df_feed_catalog = pd.DataFrame(
            execute_fetchall(QUERY_1),
            columns=(
                "id",
                "feed_id",
                "data_source",
                "data_type",
                "feed_type",
                "pipeline",
            ),
        )

        df_required_fields = pd.DataFrame(
            execute_fetchall(QUERY_2), columns=("id", "required_fields", "pipeline")
        )

        df_field_name_variation = pd.DataFrame(
            execute_fetchall(QUERY_3), columns=("id", "field_variation",
                                                "field_name", "pipeline")
        )
        for item in list(df_pipeline["pipeline"]):
            temp_list = df_feed_catalog.loc[
                df_feed_catalog["pipeline"] == item
            ].values.tolist()

            for data_feed_info in temp_list:

                temp_list = df_pipeline.loc[
                    df_pipeline["pipeline"] == item
                ]["microservice_list"].tolist()

                if len(temp_list) != 0:
                    microservice_list = str(temp_list[0]).split(",")
                reqd_df = df_required_fields.loc[
                    df_required_fields["pipeline"] == item
                ]
                reqd_list = reqd_df["required_fields"].tolist()
                if item == "NON-PII":
                    microservice_list = []
                    reqd_df = df_required_fields[
                        df_required_fields["pipeline"].isna()
                    ]
                    reqd_list = reqd_df["required_fields"].tolist()
                fname_temp_list = df_field_name_variation.loc[
                    df_field_name_variation["pipeline"] == item
                ][["field_variation", "field_name"]].values.tolist()
                fname_list = []
                logging.info("fname_temp_list %s", fname_temp_list)
                for temp_list in fname_temp_list:
                    fname_dic = {
                        "field_variation": temp_list[0],
                        "field_name": temp_list[1],
                    }
                    fname_list.append(fname_dic)

                arr_test.append({
                    "data_feed_catalog_id": data_feed_info[0],
                    "feed_id": data_feed_info[1],
                    "data_source": data_feed_info[2],
                    "data_type": data_feed_info[3],
                    "feed_type": data_feed_info[4],
                    "cdp_pipeline": {
                        "pipeline": item,
                        "microservice_list": microservice_list,
                        "cdp_required_fields": reqd_list,
                    },
                    "cdp_field_name_variations": fname_list,
                })
        fo.write_config_file(CONFIG_PATH, arr_test)

    elif SELECTED_QUERY == "NOSQL":
        fo.write_config_file(CONFIG_PATH, execute_fetchall())

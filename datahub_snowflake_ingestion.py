import os
import sys

from datahub_provider.hooks.datahub import DatahubGenericHook

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "scripts"))
from datetime import datetime, timedelta, timezone

# import common_setup
from airflow import DAG
from airflow.utils.dates import days_ago

try:
    from airflow.operators.python import PythonOperator
except ImportError:
    from airflow.operators.python_operator import PythonOperator

from datahub.ingestion.run.pipeline import Pipeline

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "execution_timeout": timedelta(minutes=10),
}


def ingest_from_snowflake(snowflake_conn_id, datahub_rest_conn_id, snowflake_db):
    snowflake_conn = DatahubGenericHook.get_connection(snowflake_conn_id)
    datahub_conn = DatahubGenericHook.get_connection(datahub_rest_conn_id)

    start_time = datetime.now(tz=timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    ) - timedelta(days=30)

    pipeline = Pipeline.create(
        {
            "source": {
                "type": "snowflake",
                "config": {
                    "username": snowflake_conn.login,
                    "password": snowflake_conn.password,
                    "account_id": snowflake_conn.extra_dejson["extra__snowflake__account"]+"."+snowflake_conn.extra_dejson["extra__snowflake__region"],
                    "warehouse": snowflake_conn.extra_dejson["extra__snowflake__warehouse"],
                    "database_pattern": {"allow": [snowflake_db]},
                    "table_pattern": {"allow": ["TRIPS"]},
                    "role": snowflake_conn.extra_dejson["extra__snowflake__role"],
                    "bucket_duration": "DAY",
                    "start_time": start_time,
                    "profiling": {"enabled": True},
                },
            },
            "sink": {
                "type": "datahub-rest",
                "config": {"server": 'http://localhost:8080'},
            },
        }
    )
    pipeline.run()
    pipeline.pretty_print_summary()
    pipeline.raise_from_status()


# def ingest_usage_from_snowflake(snowflake_conn_id, datahub_rest_conn_id):
#     snowflake_conn = DatahubGenericHook.get_connection(snowflake_conn_id)
#     datahub_conn = DatahubGenericHook.get_connection(datahub_rest_conn_id)

#     start_time = datetime.now(tz=timezone.utc).replace(
#         hour=0, minute=0, second=0, microsecond=0
#     ) - timedelta(days=30)

#     pipeline = Pipeline.create(
#         {
#             "source": {
#                 "type": "snowflake-usage",
#                 "config": {
#                     "username": snowflake_conn.login,
#                     "password": snowflake_conn.password,
#                     "account_id": snowflake_conn.extra_dejson["extra__snowflake__account"]+"."+snowflake_conn.extra_dejson["extra__snowflake__region"],
#                     "warehouse": snowflake_conn.extra_dejson["extra__snowflake__warehouse"],
#                     "role": snowflake_conn.extra_dejson["extra__snowflake__role"],
#                     "bucket_duration": "DAY",
#                     "start_time": start_time,
#                 },
#             },
#             "sink": {
#                 "type": "datahub-rest",
#                 "config": {"server": 'http://localhost:8080'},
#             },
#         }
#     )
#     pipeline.run()
#     pipeline.pretty_print_summary()
#     pipeline.raise_from_status()

def f():
    pass


with DAG(
    "datahub_snowflake_ingestion",
    default_args=default_args,
    description="Run automated metadata ingestion from Snowflake to Datahub",
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    catchup=False,
) as dag:
    ingest_task = PythonOperator(
        task_id="ingest_from_snowflake",
        python_callable=ingest_from_snowflake,
        op_kwargs={
            "snowflake_conn_id": "snowflake_conn",
            "datahub_rest_conn_id": "datahub_rest_default",
            "snowflake_db": "^TM_INTERNAL_DB$",
        },
    )

    test_downstream = PythonOperator(
        task_id = "tmp_task",
        python_callable=f
    )

    ingest_task >> test_downstream
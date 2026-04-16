from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.operators.s3 import S3Hook
from airflow.models import Variable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.decorators import dag, task

@dag(dag_id="dev_team_1_load_weather_to_stg",
    schedule=timedelta(minutes=5),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    description="dev_team_1 Загрузка файла погоды в таблицу stg",
    tags=["dev_team_1"]
)
def weather_load_to_stg_dag():

    task_load_table_to_stg = SQLExecuteQueryOperator(task_id='sql_stg_table',
        sql = [
            "CREATE SCHEMA IF NOT EXISTS stg",
        """
            CREATE TABLE IF NOT EXISTS stg.weather (
                    icao_code TEXT,
                    local_time TIMESTAMPTZ,
                    temperature FLOAT,
                    w_speed FLOAT,
                    max_gws FLOAT,
                    w_phenomena TEXT default '',
                    processed_dttm TIMESTAMPTZ default (now()),
                    CONSTRAINT weather_pkey PRIMARY KEY (icao_code, local_time)
                )
        """,
        """
        INSERT INTO stg.weather 
        SELECT 
            icao_code,
            local_time,
            temperature,
            w_speed,
            max_gws,
            w_phenomena,
            '{{ data_interval_end }}' as processed_dttm
        FROM ods.weather
        ON CONFLICT (icao_code, local_time) DO UPDATE
        SET
              temperature = excluded.temperature,
              w_speed = excluded.w_speed,
              max_gws = excluded.max_gws,
              w_phenomena = excluded.w_phenomena,
              processed_dttm = excluded.processed_dttm
        """],
        autocommit=True,
        conn_id="con_dwh_2024_s001",
        parameters=None,
        split_statements=False,
        )


    task_load_table_to_stg

instance = weather_load_to_stg_dag()
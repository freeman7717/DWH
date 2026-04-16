from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.operators.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

@dag(dag_id="dev_team_1_load_airports_to_dds",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    description="dev_team_1 Загрузка справочника аэропортов в DDS",
    tags=["dev_team_1"]
)

def airports_load_to_dds_dag():

    task_load_table_to_dds = SQLExecuteQueryOperator(task_id='sql_dds_table',
        sql = [
        """
        CREATE SCHEMA IF NOT EXISTS dds;
        DROP TABLE IF EXISTS dds.airport;
        CREATE TABLE IF NOT EXISTS dds.airport (
            airport_rk VARCHAR(10) PRIMARY KEY,
            icao_code VARCHAR(10),
            iata_code VARCHAR(10),
            airport_type_code VARCHAR(30),
            airport_nm VARCHAR(100),
            elevation_ft FLOAT,
            country_iso_code VARCHAR(10),
            region_iso_code VARCHAR(100),
            region_nm VARCHAR(100),
            city_nm VARCHAR(100),
            city_code VARCHAR(10),
            time_zone VARCHAR(100),
            processed_dttm TIMESTAMPTZ DEFAULT NOW()
        )
        """,
        """
        INSERT INTO dds.airport (
            airport_rk,
            icao_code,
            iata_code,
            airport_type_code,
            airport_nm,
            elevation_ft,
            country_iso_code,
            region_iso_code,
            region_nm,
            city_nm,
            city_code,
            time_zone
        )
        SELECT distinct on (icao_code)
            icao_code,
            icao_code,
            iata_code,
            type,
            name,
            elevation_ft,
            iso_country,
            iso_region,
            region_name,
            city_name,
            city_code,
            time_zone
        FROM ods.airport
        WHERE icao_code IS NOT NULL AND icao_code != ''
        order by icao_code;
        """],
        autocommit=True,
        conn_id="con_dwh_2024_s001",
        parameters=None,
        split_statements=False,
        )

    task_load_table_to_dds

instance = airports_load_to_dds_dag()
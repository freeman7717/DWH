from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.operators.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

s3_bucket = 'gsbdwhdata'
s3_location = 'dev_team_1/flights/airports.csv'

@dag(dag_id="dev_team_1_load_airports_to_ods",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    description="dev_team_1 Загрузка справочника аэропортов в ODS",
    tags=["dev_team_1"]
)
def airport_copy_dag():
    s3_sensor = S3KeySensor(
        task_id='s3_file_check',
        poke_interval=60,
        timeout=180,
        soft_fail=False,
        retries=2,
        bucket_key=s3_location,
        bucket_name=s3_bucket,
        aws_conn_id='object_storage_yc'
    )
    
    task_create_table = SQLExecuteQueryOperator(task_id='sql_src_table',
        sql = [
            "CREATE SCHEMA IF NOT EXISTS ods",
            "DROP TABLE IF EXISTS ods.airport",
        """
            CREATE TABLE IF NOT EXISTS ods.airport (
                    icao_code TEXT,
                    iata_code TEXT,
                    type TEXT,
                    name TEXT,
                    elevation_ft INTEGER,
                    iso_country TEXT,
                    iso_region TEXT,
                    region_name TEXT,
                    city_name TEXT,
                    city_code TEXT,
                    time_zone  TEXT
                )
        """],
        autocommit=True,
        conn_id="con_dwh_2024_s001",
        parameters=None,
        split_statements=False,
        )
    
    @task()
    def write_file_to_db():
        s3_hook = S3Hook(aws_conn_id='object_storage_yc')
        file_name = s3_hook.download_file(key=s3_location, bucket_name=s3_bucket)
        
        if file_name:
            postgres_hook = PostgresHook(postgres_conn_id="con_dwh_2024_s001")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            with open(file_name, "r") as file:
                cur.copy_expert(
                    "COPY ods.airport FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                    file,
                )
            conn.commit()


    task_create_table >> write_file_to_db()

instance = airport_copy_dag()
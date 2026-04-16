from calendar import month
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.s3 import S3Hook
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor


year_month = Variable.get("dev_team_1_flights_year_month")
source_bucket = 'db01-content'
source_key = f'flights/T_ONTIME_REPORTING-{year_month}.csv'

@dag(dag_id="dev_team_1_load_flights_to_ods",
     schedule=None,
     start_date=datetime(2024, 1, 1),
     catchup=False,
     description="dev_team_1 Копируем полёты за определенный год и месяц в ODS",
     tags=["dev_team_1"]
)

def copy_flights_dag():
    s3_sensor = S3KeySensor(
        task_id='s3_file_check',
        poke_interval=60,
        timeout=180,
        soft_fail=False,
        retries=2,
        bucket_key=source_key,
        bucket_name=source_bucket,
        aws_conn_id='object_storage_yc'
    )

    task_create_table = SQLExecuteQueryOperator(task_id='sql_src_table',
        sql = [
            "CREATE SCHEMA IF NOT EXISTS ods",
            "DROP TABLE IF EXISTS ods.flights",
        """
            CREATE TABLE IF NOT EXISTS ods.flights (
                    year INTEGER,
                    month INTEGER,
                    flight_dt TEXT,
                    carrier_code TEXT DEFAULT '',
                    tail_num TEXT DEFAULT '',
                    carrier_flight_num TEXT DEFAULT '',
                    origin_code TEXT,
                    origin_city_name TEXT,
                    dest_code TEXT,
                    dest_city_name TEXT,
                    scheduled_dep_tm TEXT DEFAULT '',
                    actual_dep_tm TEXT DEFAULT '',
                    dep_delay_min float DEFAULT 0,
                    dep_delay_group_num float DEFAULT 0,
                    wheels_off_tm TEXT DEFAULT '',
                    wheels_on_tm TEXT DEFAULT '',
                    scheduled_arr_tm TEXT DEFAULT '',
                    actual_arr_tm TEXT DEFAULT '',
                    arr_delay_min float DEFAULT 0,
                    arr_delay_group_num float DEFAULT 0,
                    cancelled_flg float DEFAULT 0,
                    cancellation_code TEXT DEFAULT '',
                    flights_cnt float DEFAULT 0,
                    distance float DEFAULT 0,
                    distance_group_num float DEFAULT 0,
                    carrier_delay_min float DEFAULT 0,
                    weather_delay_min float DEFAULT 0,
                    nas_delay_min float DEFAULT 0,
                    security_delay_min float DEFAULT 0,
                    late_aircraft_delay_min float DEFAULT 0,
                    processed_dttm timestamptz not null default now()
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
        file_name = s3_hook.download_file(key=source_key, bucket_name=source_bucket)
        
        if file_name:
            postgres_hook = PostgresHook(postgres_conn_id="con_dwh_2024_s001")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            with open(file_name, "r") as file:
                cur.copy_expert(
                    """COPY ods.flights (
                    year,
                    month,
                    flight_dt,
                    carrier_code,
                    tail_num,
                    carrier_flight_num,
                    origin_code,
                    origin_city_name,
                    dest_code,
                    dest_city_name,
                    scheduled_dep_tm,
                    actual_dep_tm,
                    dep_delay_min,
                    dep_delay_group_num,
                    wheels_off_tm,
                    wheels_on_tm,
                    scheduled_arr_tm,
                    actual_arr_tm,
                    arr_delay_min,
                    arr_delay_group_num,
                    cancelled_flg,
                    cancellation_code,
                    flights_cnt,
                    distance,
                    distance_group_num,
                    carrier_delay_min,
                    weather_delay_min,
                    nas_delay_min,
                    security_delay_min,
                    late_aircraft_delay_min
                    )
                    FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'""",
                    file,
                )
            conn.commit()


    s3_sensor >> task_create_table >> write_file_to_db()

instance = copy_flights_dag()
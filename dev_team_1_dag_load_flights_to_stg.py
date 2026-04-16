from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.operators.s3 import S3Hook
from airflow.models import Variable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.decorators import dag, task

@dag(dag_id="dev_team_1_load_flight_to_stg",
    schedule=timedelta(minutes=5),
    start_date=datetime(2025, 5, 25),
    catchup=False,
    description="dev_team_1 Загрузка файла полетов в таблицу stg",
    tags=["dev_team_1"]
)
def flights_load_to_stg_dag():


    task_load_table_to_stg = SQLExecuteQueryOperator(task_id='sql_stg_table',
        sql = [
            "CREATE SCHEMA IF NOT EXISTS stg",
        """
            CREATE TABLE IF NOT EXISTS stg.flights (
                    year INTEGER,
                    month INTEGER,
                    flight_dt TEXT,
                    carrier_code TEXT,
                    tail_num TEXT,
                    carrier_flight_num TEXT,
                    origin_code TEXT,
                    origin_city_name TEXT,
                    dest_code TEXT,
                    dest_city_name TEXT,
                    scheduled_dep_tm TEXT,
                    actual_dep_tm TEXT,
                    dep_delay_min int,
                    dep_delay_group_num int,
                    wheels_off_tm TEXT,
                    wheels_on_tm TEXT,
                    scheduled_arr_tm TEXT,
                    actual_arr_tm TEXT,
                    arr_delay_min int,
                    arr_delay_group_num int,
                    cancelled_flg int,
                    cancellation_code TEXT,
                    flights_cnt int,
                    distance int,
                    distance_group_num int,
                    carrier_delay_min int,
                    weather_delay_min int,
                    nas_delay_min int,
                    security_delay_min int,
                    late_aircraft_delay_min int,
                    processed_dttm timestamptz not null default now(),
                    primary key (flight_dt, scheduled_dep_tm, carrier_code,  carrier_flight_num)
                )
        """,
        """
        insert into stg.flights
            select year, month, flight_dt, carrier_code, tail_num, carrier_flight_num,
            origin_code, origin_city_name, dest_code, dest_city_name,
            scheduled_dep_tm, actual_dep_tm, dep_delay_min, dep_delay_group_num,
            wheels_off_tm, wheels_on_tm, scheduled_arr_tm, actual_arr_tm,
            arr_delay_min, arr_delay_group_num, cancelled_flg, cancellation_code,
            flights_cnt, distance, distance_group_num, carrier_delay_min,
            weather_delay_min, nas_delay_min, security_delay_min, late_aircraft_delay_min, 
            '{{ data_interval_end }}' as processed_dttm
            from ods.flights
            where 
                ods.flights.processed_dttm >= '{{ data_interval_start }}'
                    and ods.flights.processed_dttm < '{{ data_interval_end }}'
        
            on conflict(flight_dt, scheduled_dep_tm, carrier_code, carrier_flight_num) do update
            set (
            year, month, flight_dt, carrier_code, tail_num, carrier_flight_num,
            origin_code, origin_city_name, dest_code, dest_city_name,
            scheduled_dep_tm, actual_dep_tm, dep_delay_min, dep_delay_group_num,
            wheels_off_tm, wheels_on_tm, scheduled_arr_tm, actual_arr_tm,
            arr_delay_min, arr_delay_group_num, cancelled_flg, cancellation_code,
            flights_cnt, distance, distance_group_num, carrier_delay_min,
            weather_delay_min, nas_delay_min, security_delay_min, late_aircraft_delay_min, 
            processed_dttm
            ) =
            (
            EXCLUDED.year, EXCLUDED.month, EXCLUDED.flight_dt, EXCLUDED.carrier_code, EXCLUDED.tail_num, EXCLUDED.carrier_flight_num,
            EXCLUDED.origin_code, EXCLUDED.origin_city_name, EXCLUDED.dest_code, EXCLUDED.dest_city_name,
            EXCLUDED.scheduled_dep_tm, EXCLUDED.actual_dep_tm, EXCLUDED.dep_delay_min, EXCLUDED.dep_delay_group_num,
            EXCLUDED.wheels_off_tm, EXCLUDED.wheels_on_tm, EXCLUDED.scheduled_arr_tm, EXCLUDED.actual_arr_tm,
            EXCLUDED.arr_delay_min, EXCLUDED.arr_delay_group_num, EXCLUDED.cancelled_flg, EXCLUDED.cancellation_code,
            EXCLUDED.flights_cnt, EXCLUDED.distance, EXCLUDED.distance_group_num, EXCLUDED.carrier_delay_min,
            EXCLUDED.weather_delay_min, EXCLUDED.nas_delay_min, 
            EXCLUDED.security_delay_min, EXCLUDED.late_aircraft_delay_min,
            EXCLUDED.processed_dttm
        );
        """],
        autocommit=True,
        conn_id="con_dwh_2024_s001",
        parameters=None,
        split_statements=False,
        )
    
    task_load_table_to_stg

instance = flights_load_to_stg_dag()
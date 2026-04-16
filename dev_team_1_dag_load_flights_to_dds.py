from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.operators.s3 import S3Hook
from airflow.models import Variable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.decorators import dag, task

@dag(dag_id="dev_team_1_load_flights_to_dds",
    schedule=timedelta(minutes=5),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    description="dev_team_1 Загрузка полётов в таблицы dds",
    tags=["dev_team_1"]
)
def flights_load_to_dds_dag():

    task_load_table_to_dds = SQLExecuteQueryOperator(task_id='sql_dds_table',
        sql = [
            "CREATE SCHEMA IF NOT EXISTS dds",
        """
            CREATE TABLE IF NOT EXISTS dds.completed_flights (
                carrier_flight_num TEXT NOT NULL,
                flight_dttm_local  TIMESTAMPTZ NOT NULL,
                origin_airport_dk  TEXT NOT NULL,
                dest_airport_dk    TEXT,
                carrier_code       TEXT,
                tail_num           TEXT,
                distance_m         FLOAT,
                dep_delay_min      INTEGER,
                delay_reasons_code TEXT,
                processed_dttm     TIMESTAMP default now(),
                PRIMARY KEY (carrier_flight_num, flight_dttm_local, origin_airport_dk, carrier_code)
            );
        """,
        """
            CREATE TABLE IF NOT EXISTS dds.cancelled_flights (
                carrier_flight_num TEXT NOT NULL,
                sched_dttm_local   TIMESTAMPTZ NOT NULL,
                origin_airport_dk  TEXT NOT NULL,
                dest_airport_dk    TEXT,
                carrier_code       TEXT,
                cancellation_code  TEXT,
                processed_dttm     TIMESTAMP default now(),
                PRIMARY KEY (carrier_flight_num, sched_dttm_local, origin_airport_dk)
            );
        """,
            # Вставка новых записей
        """
            INSERT INTO dds.completed_flights (
              carrier_flight_num,
              flight_dttm_local,
              origin_airport_dk,
              dest_airport_dk,
              carrier_code,
              tail_num,
              distance_m,
              dep_delay_min,
              delay_reasons_code,
              processed_dttm
            )
            select
                s.carrier_flight_num,
                (TO_TIMESTAMP(
                   SUBSTRING(s.flight_dt FROM 1 FOR 10) 
                   || ' ' || 
                   SUBSTRING(s.scheduled_dep_tm FROM 1 FOR 2) 
                   || ':' || 
                   SUBSTRING(s.scheduled_dep_tm FROM 3 FOR 2),
                   'MM/DD/YYYY HH24:MI')  
                AT TIME ZONE a_origin.time_zone) AT TIME ZONE 'UTC',
                s.origin_code,
                s.dest_code,
                s.carrier_code,
                s.tail_num,
                s.distance,
                s.carrier_delay_min + s.weather_delay_min + s.nas_delay_min +
                s.security_delay_min + s.late_aircraft_delay_min,
                CONCAT(
                CASE WHEN s.carrier_delay_min > 0 THEN 'Carrier ' ELSE '' END,
                CASE WHEN s.weather_delay_min > 0 THEN 'Weather ' ELSE '' END,
                CASE WHEN s.nas_delay_min > 0 THEN 'NAS ' ELSE '' END,
                CASE WHEN s.security_delay_min > 0 THEN 'Security ' ELSE '' END,
                CASE WHEN s.late_aircraft_delay_min > 0 THEN 'Late_aircraft' ELSE '' END
                ) AS delay_reasons_code,
                '{{ data_interval_end }}' as processed_dttm
            FROM stg.flights s
            JOIN dds.airport a_origin ON s.origin_code = a_origin.iata_code 
            WHERE s.cancelled_flg = 0
                and s.processed_dttm >= '{{ data_interval_start }}'
                and s.processed_dttm < '{{ data_interval_end }}'
            on conflict(carrier_flight_num, flight_dttm_local, origin_airport_dk, carrier_code) do update
            set (
                carrier_flight_num,
                flight_dttm_local,
                origin_airport_dk,
                dest_airport_dk,
                carrier_code,
                tail_num,
                distance_m,
                dep_delay_min,
                delay_reasons_code,
                processed_dttm
            ) =
            (
            EXCLUDED.carrier_flight_num,
                EXCLUDED.flight_dttm_local,
                EXCLUDED.origin_airport_dk,
                EXCLUDED.dest_airport_dk,
                EXCLUDED.carrier_code,
                EXCLUDED.tail_num,
                EXCLUDED.distance_m,
                EXCLUDED.dep_delay_min,
                EXCLUDED.delay_reasons_code,
                EXCLUDED.processed_dttm
            );
        """,
        """
        INSERT INTO dds.cancelled_flights (
          carrier_flight_num,
          sched_dttm_local,
          origin_airport_dk,
          dest_airport_dk,
          carrier_code,
          cancellation_code,
          processed_dttm
        )
        SELECT
          s.carrier_flight_num,
          (TO_TIMESTAMP(
               SUBSTRING(s.flight_dt FROM 1 FOR 10) 
               || ' ' || 
               SUBSTRING(s.scheduled_dep_tm FROM 1 FOR 2) 
               || ':' || 
               SUBSTRING(s.scheduled_dep_tm FROM 3 FOR 2),
               'MM/DD/YYYY HH24:MI')  
           AT TIME ZONE a_origin.time_zone) AT TIME ZONE 'UTC',
          s.origin_code,
          s.dest_code,
          s.carrier_code,
          s.cancellation_code,
          '{{ data_interval_end }}' as processed_dttm
        FROM stg.flights s
        JOIN dds.airport a_origin ON s.origin_code = a_origin.iata_code
        WHERE s.cancelled_flg = 1
            and s.processed_dttm >= '{{ data_interval_start }}'
            and s.processed_dttm < '{{ data_interval_end }}'
        on conflict(carrier_flight_num, sched_dttm_local, origin_airport_dk) do update
        set (
          carrier_flight_num,
          sched_dttm_local,
          origin_airport_dk,
          dest_airport_dk,
          carrier_code,
          cancellation_code,
          processed_dttm
        ) =
        (
          EXCLUDED.carrier_flight_num,
          EXCLUDED.sched_dttm_local,
          EXCLUDED.origin_airport_dk,
          EXCLUDED.dest_airport_dk,
          EXCLUDED.carrier_code,
          EXCLUDED.cancellation_code,
          EXCLUDED.processed_dttm
        );
        """],
        autocommit=True,
        conn_id="con_dwh_2024_s001",
        parameters=None,
        split_statements=False,
        )


    task_load_table_to_dds

instance = flights_load_to_dds_dag()

from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.operators.s3 import S3Hook
from airflow.models import Variable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.decorators import dag, task

@dag(dag_id="dev_team_1_load_weather_to_dds",
    schedule=timedelta(minutes=5),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    description="dev_team_1 Загрузка файла погоды в таблицу dds",
    tags=["dev_team_1"]
)
def weather_load_to_dds_dag():

    task_load_table_to_dds = SQLExecuteQueryOperator(task_id='sql_dds_table',
        sql = [
            "CREATE SCHEMA IF NOT EXISTS dds",
        """
            CREATE TABLE IF NOT EXISTS dds.weather (
                airport_rk       TEXT,
                w_speed          FLOAT,
                max_gws          FLOAT,
                t_deg            FLOAT,
                w_phenomena      TEXT,
                valid_from_dttm  TIMESTAMPTZ,
                valid_to_dttm    TIMESTAMPTZ,
                processed_dttm   TIMESTAMPTZ default(now()),
                PRIMARY KEY(airport_rk, valid_from_dttm)
            );
        """,
        # Удаление старых данных, где совпадают airport_rk и время с учетом временной зоны
        """
            DELETE FROM dds.weather d
            USING dds.airport a, stg.weather s
            WHERE a.icao_code = s.icao_code
                AND d.airport_rk = a.iata_code
                AND d.valid_from_dttm = (s.local_time AT TIME ZONE a.time_zone) AT TIME ZONE 'UTC'
                and s.processed_dttm >= '{{ data_interval_start }}'
                and s.processed_dttm < '{{ data_interval_end }}'

            ;
        """,
            # Вставка новых записей
        """
            INSERT INTO dds.weather (
                airport_rk,
                w_speed,
                max_gws,
                t_deg,
                w_phenomena,
                valid_from_dttm,
                valid_to_dttm,
                processed_dttm
            )
            SELECT
                a.iata_code AS airport_rk,
                s.w_speed,
                COALESCE(s.max_gws, 0) AS max_gws,
                s.temperature AS t_deg, 
                s.w_phenomena,
                -- Преобразование local_time в UTC
                (s.local_time AT TIME ZONE a.time_zone) AT TIME ZONE 'UTC' AS valid_from_dttm,
                -- Следующее значение времени (если есть), тоже приведенное к UTC
                COALESCE(
                        LEAD((s.local_time AT TIME ZONE a.time_zone) AT TIME ZONE 'UTC')
                        OVER (PARTITION BY a.icao_code ORDER BY s.local_time),
                        '5999-01-01'::TIMESTAMPTZ AT TIME ZONE 'UTC'
                    ) AS valid_to_dttm,
                '{{ data_interval_end }}' AS processed_dttm
            FROM stg.weather s
            JOIN dds.airport a ON a.icao_code = s.icao_code
            where s.processed_dttm >= '{{ data_interval_start }}'
                  and s.processed_dttm < '{{ data_interval_end }}'
            ;
        """],
        autocommit=True,
        conn_id="con_dwh_2024_s001",
        parameters=None,
        split_statements=False,
        )


    task_load_table_to_dds

instance = weather_load_to_dds_dag()
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

airports = Variable.get("dev_team_1_airport_weather")
your_name = 'dev_team_1'  # имя пользователя для пути
source_bucket = "gsbdwhdata"
destination_bucket = "gsbdwhdata"

@dag(
    dag_id="dev_team_1_load_weather_to_ods",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    description="dev_team_1 Копирование файла погоды из указанных аэропортов и удаление заголовков",
    tags=["dev_team_1"]
)
def load_weather_dag():

    task_create_table = SQLExecuteQueryOperator(task_id='sql_ods_table',
        sql = [
            "CREATE SCHEMA IF NOT EXISTS ods",
            "DROP TABLE IF EXISTS ods.weather",
        """
            CREATE TABLE IF NOT EXISTS ods.weather (
                    icao_code TEXT,
                    local_time TIMESTAMPTZ,
                    temperature FLOAT,
                    w_speed FLOAT,
                    max_gws FLOAT,
                    w_phenomena TEXT default '',
                    processed_dttm timestamptz not null default now(),
                    CONSTRAINT weather_pkey PRIMARY KEY (icao_code, local_time)
                )
        """],
        autocommit=True,
        conn_id="con_dwh_2024_s001",
        parameters=None,
        split_statements=False,
        )

    def load_data_from_s3(airport):
        source_key = f"weather/{airport}_weather.csv"
        destination_key = f"{your_name}/flights/{airport}_weather.csv"

        print(f"Файл {source_bucket}/{source_key}")

        # Получаем S3 Hook с конфигурацией из подключения object_storage_yc
        s3_hook = S3Hook(aws_conn_id="object_storage_yc")

        print(f"Файл read")
        file_path = s3_hook.download_file(key=source_key, bucket_name=source_bucket)
        print(f"Файл read complete")
        print(f"Файл remove header")

        with open(file_path, "r", encoding='utf-8') as file:
            lines = file.readlines()

        data_list = []
        for line in lines[6:]: # пропускаем заголовок и обрабатывает
            parts = [x for x in line.replace('"', '').split(';')]
            data_list.append(f"{airport};{parts[0]};{parts[1]};{parts[6]};{parts[7]};{parts[8]}")
        data = '\n'.join(data_list)

        print(f'Файл remove header complete')

        # Перезаписываем файл в целевой бакет
        s3_hook.load_string(
            string_data=data,
            key= destination_key,
            bucket_name= destination_bucket,
            replace=True
        )
        print("Файл weather загружен")

        file_name = s3_hook.download_file(key=destination_key, bucket_name=destination_bucket)

        if file_name:
            postgres_hook = PostgresHook(postgres_conn_id="con_dwh_2024_s001")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            with open(file_name, "r") as file:
                cur.copy_expert(
                    """COPY ods.weather (
                        icao_code,
                        local_time,
                        temperature,
                        w_speed,
                        max_gws,
                        w_phenomena
                    ) FROM STDIN WITH CSV HEADER DELIMITER AS ';' QUOTE '\"'
                    """,
                    file,
                )
            conn.commit()
        else:
            print(f'Файл {destination_key}/{destination_bucket} не найден')

    @task
    def load_weather_for_all_airports():
        airports_list = [a for a in airports.split()]
        for airport in airports_list:
            load_data_from_s3(airport)



    task_create_table >> load_weather_for_all_airports()

instance = load_weather_dag()



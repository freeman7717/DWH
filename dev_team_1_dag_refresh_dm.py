from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.operators.s3 import S3Hook
from airflow.models import Variable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.decorators import dag, task

@dag(dag_id="dev_team_1_refresh_dm",
    schedule=timedelta(minutes=5),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    description="dev_team_1 Обновление витрин данных",
    tags=["dev_team_1"]
)
def refresh_dm_dag():

    task_refresh_dm = SQLExecuteQueryOperator(task_id='sql_refresh_dm',
        sql = [
            """
                REFRESH MATERIALIZED VIEW dm.mv_cancelled_base
            """,
            """
                REFRESH MATERIALIZED VIEW dm.mv_cancelled_due_weather_fact
            """],
        autocommit=True,
        conn_id="con_dwh_2024_s001",
        parameters=None,
        split_statements=False,
        )


    task_refresh_dm

instance = refresh_dm_dag()
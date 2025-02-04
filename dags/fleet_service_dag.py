from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from etl.fleet_excel_to_snowflake_etl import fleet_excel_to_snowflake_etl
from etl.fleet_new_task import fleet_new_task_function
from etl.ETL_S3_SNOWFLAKES import execute_snowflake_sql
from etl.sf_to_postgresql import sf_to_postgresql
from dotenv import load_dotenv
load_dotenv()

with DAG(
    dag_id="fleet_service",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:
    etl_task = PythonOperator(
        task_id="fleet_excel_to_snowflake_etl",
        python_callable=fleet_excel_to_snowflake_etl,
    )

    new_task = PythonOperator(
        task_id="fleet_new_task",
        python_callable=fleet_new_task_function
    )

    # Set task dependencies
    etl_task >> new_task


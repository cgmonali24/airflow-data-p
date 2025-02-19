from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import os
import re
from datetime import datetime, timedelta
import glob
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'ecom_modules')))
from validate import validate_csv
from clean import clean_data
from store import store_in_db


from airflow.models import Variable
from constant import VALIDATION_FORMATS, FORMAT_PATTERNS, DATE_FORMAT_MAPPING
date_format = Variable.get("date_format", default_var="YYYY-MM-DD")
CSV_DIR = Variable.get("csv_dir", default_var="/opt/airflow/dags/tmp/")
POSTGRES_CONNECTION_ID = Variable.get("postgres_conn_id")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 9),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


dag = DAG(
    "csv_data_analysis",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)


wait_for_file = FileSensor(
    task_id="wait_for_csv_file",
    filepath=CSV_DIR + "*.csv", 
    poke_interval=10, 
    timeout=3600,  
    mode="poke",  
    dag=dag,
)


validate_task = PythonOperator(
    task_id="validate_csv",
    python_callable=validate_csv,
    op_kwargs={"CSV_DIR": CSV_DIR},
    dag=dag,
)


cleanup_task = PythonOperator(
    task_id="clean_data",
    python_callable=clean_data,
    dag=dag,
    provide_context=True,
)


store_task = PythonOperator(
    task_id="store_in_db",
    python_callable=store_in_db,
    dag=dag,
    provide_context=True,
    op_kwargs={"POSTGRES_CONNECTION_ID": POSTGRES_CONNECTION_ID},
)


wait_for_file >> validate_task >> cleanup_task >> store_task

import sys
import os
sys.path.append('/opt/airflow/plugins')

from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.fetch_and_upload import fetch_and_upload_by_date  # 새로 만든 함수
import pendulum
from datetime import datetime, timedelta


def fetch_and_upload_upbit_data_full_year():
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 1, 31)
    current_date = start_date

    while current_date <= end_date:
        execution_date = current_date.strftime('%Y-%m-%dT00:00:00')
        print(f"📅 {execution_date} 처리 시작")

        fetch_and_upload_by_date(execution_date)
        current_date += timedelta(days=1)


with DAG(
    dag_id = 'dags_backfill_candlestick_data',
    start_date = pendulum.datetime(2024, 1, 1, tz='Asia/Seoul'),
    schedule_interval = '@once',
    catchup = False,
    tags = ['candlestick', 'backfill']
) as dag:

    fetch_all_task = PythonOperator(
        task_id='fetch_and_upload_upbit_full_year',
        python_callable=fetch_and_upload_upbit_data_full_year
    )

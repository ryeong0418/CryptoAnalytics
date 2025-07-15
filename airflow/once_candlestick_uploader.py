import sys
import os
from datetime import datetime, timedelta
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts.upload_to_storage import upload_to_blob_storage
from scripts.candlestick_daily import CandleStickDailyOperator


def run_candlestick_and_upload(execution_date:str):
    print(f"✅ Processing: {execution_date}")
    CandleStickDailyOperator().execute(execution_date=execution_date)
    upload_to_blob_storage(
        market_url="",
        execution_date=execution_date
    )

with DAG(
    dag_id="once_candlestick_uploader",
    start_date=pendulum.datetime(2024,1,1,tz="Asia/Seoul"),
    schedule="@once",
    catchup=False,
    tags=["upbit", "2024", "by_date"],
    description="candlestick date 순차 처리",
    max_active_tasks=1
)as dag:

    start = datetime(2024,1,1)
    end = datetime(2024,1,31)
    current = start

    while current <= end:
        date_str = current.strftime('%Y-%m-%d')

        PythonOperator(
            task_id=f"process_{date_str}",
            python_callable=run_candlestick_and_upload,
            op_args=[date_str],
        )

        current += timedelta(days=1)
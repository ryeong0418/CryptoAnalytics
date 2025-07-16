import os
import sys
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

# 사용자 정의 모듈 경로 추가
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
# from scripts.upload_to_storage import upload_to_blob_storage
from scripts.candlestick_test import CandleStickDaily


with DAG(
    dag_id="task_upload_blob_test",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    end_date=pendulum.datetime(2024, 1, 10, tz="Asia/Seoul"),
    max_active_runs=1,
    tags=["upload", "candlestick"],
) as dag:

    candlestick_task = CandleStickDaily(
        task_id="candlestick_task"
    )
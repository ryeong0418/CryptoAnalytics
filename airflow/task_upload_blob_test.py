import os
import sys
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

# 사용자 정의 모듈 경로 추가
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
# from scripts.upload_to_storage import upload_to_blob_storage
from scripts.common.utils import SystemUtils
import requests
import json
import time

def get_market_list(**context):
    execution_date = context["ds"]
    print(f"실행일자: {execution_date}")

    all_market = "https://api.upbit.com/v1/market/all"
    market_list = SystemUtils.get_market_list(all_market)
    print("마켓 리스트")
    print(market_list)

    return market_list


with DAG(
    dag_id="task_upload_blob_test",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    end_date=pendulum.datetime(2024, 1, 10, tz="Asia/Seoul"),
    max_active_runs=1,
    catchup=True,
    tags=["upload", "candlestick"],
) as dag:

    candlestick_task = PythonOperator(
        task_id="candlestick_task",
        python_callable=get_market_list
    )



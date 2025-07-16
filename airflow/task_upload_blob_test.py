import os
import sys
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

# 사용자 정의 모듈 경로 추가
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from scripts.upload_to_storage import upload_to_blob_storage
from scripts.candlestick_daily import CandleStickDailyOperator

default_args = {
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=5),
}

with DAG(
    dag_id="task_upload_blob_test",
    default_args=default_args,
    schedule="@daily",  # 매일 자정 실행
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    end_date=pendulum.datetime(2024, 1, 10, tz="Asia/Seoul"),  # 12월 31일까지
    max_active_runs=1,  # DAG 실행은 한 번에 하나만
    tags=["upload", "candlestick"],
) as dag:

    candlestick_task = CandleStickDailyOperator(
        task_id="candlestick_task"
        #execution_date="{{ ds }}",  # Jinja 템플릿으로 실행 날짜 주입
    )

    upload_blob_task = PythonOperator(
        task_id="upload_blob_task",
        python_callable=upload_to_blob_storage,
        op_kwargs={
            "market_url": "https://api.upbit.com/v1/market/all",
            #"execution_date": "{{ ds }}"  # 템플릿으로 넘김
        },
    )

    candlestick_task >> upload_blob_task

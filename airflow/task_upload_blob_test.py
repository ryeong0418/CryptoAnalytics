import os
import sys
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

# 사용자 정의 모듈 경로 추가
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
# from scripts.upload_to_storage import upload_to_blob_storage
from scripts.candlestick_test import CandleStickDailyOperator


def upload_to_blob_storage(market_url, **context):
    from datetime import datetime, timedelta
    from airflow.models import Variable
    from azure.storage.blob import BlobServiceClient
    from scripts.common.utils import SystemUtils

    execution_date = context["ds"]
    previous_execution_date = (datetime.strptime(execution_date, "%Y-%m-%d") - timedelta(days=1)).strftime(
        "%Y-%m-%d")

    data = context["ti"].xcom_pull(task_ids="candlestick_task", map_index=None)

    conn_str = Variable.get('azure_storage_connection_string')
    container_name = 'analyticscontainer'
    blob_service_client = BlobServiceClient.from_connection_string(conn_str)
    container_client = blob_service_client.get_container_client(container_name)

    market_list = SystemUtils.get_market_list(market_url)

    for market in market_list:
        filename = f'{market}-{previous_execution_date}.json'
        storage_path = f"{market}/{filename}"
        try:
            blob_client = container_client.get_blob_client(storage_path)
            market_data = data.get(market, None)
            blob_client.upload_blob(market_data.encode('utf-8'), blob_type="BlockBlob", overwrite=True)
            print(f"✅ 업로드 완료: {storage_path}")
        except Exception as e:
            print(f"❌ 업로드 실패: {storage_path} - {e}")


with DAG(
    dag_id="task_upload_blob_test",
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

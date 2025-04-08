import sys
import os
#절대 경로 추가
sys.path.append('/opt/airflow/plugins')

from airflow import DAG
from scripts.upload_to_storage import upload_to_blob_storage
from scripts.candlestick_data import CandleStick_Daily
from airflow.operators.python import PythonOperator
import pendulum
from datetime import datetime, timedelta


def fetch_and_upload_upbit_data_full_year():
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 1, 10)

    cs = CandleStick_Daily()
    current_date = start_date

    while current_date <= end_date:
        execution_date = current_date.strftime('%Y-%m-%dT00:00:00')
        print(f'수집중:{execution_date}')

        try:
            data = cs.extract_daily_data(execution_date)

            # TODO: 저장 또는 업로드 처리
            filename = f"{current_date.strftime('%Y-%m-%d')}.json"
            print('filename', filename)
            upload_to_blob_storage(data, filename, directory='candlestick-storage')

            print(f'{execution_date} 데이터 저장 완료')

        except Exception as e:
            print(f'{execution_date} 데이터 수집 실패')

        current_date += timedelta(days=1)

with DAG(
    dag_id='dags_backfill_candlestick_data',
    start_date=pendulum.datetime(2024,1,1,tz='Asia/Seoul'),
    schedule_interval='@once',
    catchup=False,
    tags=['candlestick', 'backfill']
) as dag:

    fetch_all_task = PythonOperator(
        task_id='fetch_upbit_candlestick_full_year',
        python_callable=fetch_and_upload_upbit_data_full_year
    )

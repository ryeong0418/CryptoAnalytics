import sys
import os

sys.path.append('/opt/airflow/plugins')

from airflow import DAG
from scripts.upload_to_storage import upload_to_blob_storage
from scripts.candlestick_data import CandleStick_Daily
from airflow.operators.python import PythonOperator
import pendulum


with DAG(
    dag_id='dags_candlestick_daily',
    start_date=pendulum.datetime(2023,3,1,tz='Asia/Seoul'),
    catchup=False,
    schedule_interval='* * * * *'

) as dag:

    start_date = pendulum.datetime(2024, 6, 1, tz='Asia/Seoul')
    end_date = pendulum.datetime(2024, 6, 10, tz='Asia/Seoul')
    specified_date = start_date

    while specified_date < end_date:

        date_str = specified_date.format('YYYY-MM-DD')

        candlestick_daily_data = PythonOperator(
            task_id=f"candlestick_daily_data_{date_str}",
            python_callable=CandleStick_Daily
        )

        upload_blob_task = PythonOperator(
            task_id="upload_blob_task",
            python_callable=upload_to_blob_storage,
            op_args=[date_str],
            op_kwargs={"directory":"candlestick-storage"},
            provide_context=True,
            dag=dag
        )

        candlestick_daily_data >> upload_blob_task
        specified_date = specified_date.add(days=1)

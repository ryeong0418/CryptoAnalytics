import sys
import os
#절대 경로 추가
sys.path.append('/opt/airflow/plugins')

from airflow import DAG
from scripts.upload_to_storage import upload_to_blob_storage
from scripts.candlestick_daily import CandleStickDailyOperator
from airflow.operators.python import PythonOperator
import pendulum
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime, timedelta

with DAG(
    dag_id='dags_historical_candlestick_data',
    start_date=pendulum.datetime(2024,3,1,tz='Asia/Seoul'),
    schedule_interval='@once',
    catchup=False
) as dag:

    start_date = pendulum.datetime(2024, 1, 5, tz='Asia/Seoul')
    end_date = pendulum.datetime(2024, 1, 6, tz='Asia/Seoul')
    specified_date = start_date

    while specified_date < end_date:

        execution_date = specified_date.format('YYYY-MM-DD')
        execution_date_str = (datetime.strptime(execution_date, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')

        candlestick_daily_data = CandleStickDailyOperator(
            task_id=f"candlestick_daily_data_{execution_date_str}",
            execution_date=execution_date,
            dag=dag
        )

        upload_blob_task = PythonOperator(
            task_id=f"upload_blob_task_{execution_date_str}",
            python_callable=upload_to_blob_storage,
            op_kwargs={"market_url": "https://api.upbit.com/v1/market/all",
                       "execution_date":execution_date},
            provide_context=True,
            dag=dag
        )

        candlestick_daily_data >> upload_blob_task
        specified_date = specified_date.add(days=1)
    
    # # 모든 날짜별 upload_blob_task 끝난 후 실행
    # run_databricks_job = DatabricksRunNowOperator(
    #     task_id="run_databricks_mart_job",
    #     databricks_conn_id="databricks_default",
    #     job_id=<databricks_job_id>,  # Job ID 입력
    #     notebook_params={
    #         "execution_date": "{{ ds }}"  # 필요시 전달 (원하면 고정값 가능)
    #     },
    #     dag=dag
    # )
    #
    # # 마지막 upload_blob_task → DatabricksRunNowOperator 연결
    # if last_upload_task is not None:
    #     last_upload_task >> run_databricks_job
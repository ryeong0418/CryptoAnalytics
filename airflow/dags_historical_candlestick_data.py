import sys
import os
from datetime import datetime, timedelta
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

sys.path.append('/opt/airflow/plugins')
from scripts.upload_to_storage import upload_to_blob_storage
from scripts.candlestick_daily import CandleStickDailyOperator

with (DAG(
    dag_id='dags_historical_candlestick_data',
    start_date=pendulum.datetime(2024,3,1,tz='Asia/Seoul'),
    schedule_interval='@once',
    catchup=False
) as dag):

    start_date = pendulum.datetime(2024, 1, 6, tz='Asia/Seoul')
    end_date = pendulum.datetime(2024, 1, 7, tz='Asia/Seoul')
    specified_date = start_date

    while specified_date < end_date:
        execution_date = specified_date.format('YYYY-MM-DD')
        previous_execution_date = (datetime.strptime(execution_date, '%Y-%m-%d') - timedelta(days=1)
                                   ).strftime('%Y-%m-%d')

        candlestick_daily_data = CandleStickDailyOperator(
            task_id=f"candlestick_daily_data_{previous_execution_date}",
            execution_date=execution_date,
            dag=dag
        )

        upload_blob_task = PythonOperator(
            task_id=f"upload_blob_task_{previous_execution_date}",
            python_callable=upload_to_blob_storage,
            op_kwargs={"market_url": "https://api.upbit.com/v1/market/all",
                       "execution_date":execution_date},
            provide_context=True,
            dag=dag
        )

        run_databricks_job = DatabricksRunNowOperator(
            task_id=f"run_databricks_mart_job_{previous_execution_date}",
            databricks_conn_id="databricks_connectionid",
            job_id='481122602014680',  # Job ID 입력
            notebook_params={
                "execution_date": execution_date # 필요시 전달 (원하면 고정값 가능)
            },
            dag=dag
        )

        candlestick_daily_data >> upload_blob_task >> run_databricks_job
        specified_date = specified_date.add(days=1)


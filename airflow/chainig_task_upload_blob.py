import sys
import os
from datetime import datetime, timedelta
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator


# sys.path.append('/opt/airflow/plugins')
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from scripts.upload_to_storage import upload_to_blob_storage
from scripts.candlestick_daily import CandleStickDailyOperator

with (DAG(
    dag_id='task_upload_blob',
    start_date=pendulum.datetime(2024,3,1,tz='Asia/Seoul'),
    schedule='@once',
    max_active_tasks=1,
    catchup=False
) as dag):

    start_date = pendulum.datetime(2024, 1, 1, tz='Asia/Seoul')
    end_date = pendulum.datetime(2024, 1, 5, tz='Asia/Seoul')
    specified_date = start_date
    previous_task = None

    while specified_date < end_date:
        execution_date = specified_date.format('YYYY-MM-DD')
        previous_execution_date = (datetime.strptime(execution_date, '%Y-%m-%d') - timedelta(days=1)
                                   ).strftime('%Y-%m-%d')

        candlestick_task = CandleStickDailyOperator(
            task_id=f"candlestick_task_{previous_execution_date}",
            execution_date=execution_date,
            dag=dag
        )

        upload_blob_task = PythonOperator(
            task_id=f"upload_blob_task_{previous_execution_date}",
            python_callable=upload_to_blob_storage,
            op_kwargs={"market_url": "https://api.upbit.com/v1/market/all",
                       "execution_date":execution_date},
            # provide_context=True,
            dag=dag
        )

        candlestick_task >> upload_blob_task

        if previous_task:
            print(f"{previous_task.task_id} >> {candlestick_task.task_id}")
            previous_task >> candlestick_task

        previous_task = upload_blob_task
        specified_date = specified_date.add(days=1)

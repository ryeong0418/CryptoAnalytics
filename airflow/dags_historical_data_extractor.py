from airflow import DAG
from plugins.operators.DataExtractOperator import DataExtractOperator
from scripts.upload_to_storage import upload_to_blob_storage
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.models.baseoperator import BaseOperator
import pendulum
import json

with DAG(
    dag_id='dags_historical_data_extractor',
    start_date=pendulum.datetime(2023,3,1,tz='Asia/Seoul'),
    catchup=False,
    schedule_interval='* * * * *'

) as dag:

    start_date = pendulum.datetime(2024, 6, 1, tz='Asia/Seoul')
    end_date = pendulum.datetime(2024, 6, 30, tz='Asia/Seoul')
    specified_date = start_date

    while specified_date < end_date:
        date_str = specified_date.format("")

        extract_task = DataExtractOperator(
            task_id="",
            http_conn_id="",
            endpoint="",
            dag=dag
        )

        upload_blob_task = PythonOperator(
            task_id="",
            python_callable=upload_to_blob_storage,
            op_args="",
            provide_context=True,
            dag=dag
        )

        extract_task >> upload_blob_task

        specified_date = specified_date.add(days=1)









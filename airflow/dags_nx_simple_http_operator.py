from airflow import DAG
from scripts.nx_api_to_json_operator import NxApiToJsonOperator
from azure.storage.blob import BlobServiceClient
from airflow.decorators import task
from airflow.operators.python import PythonOperator
import pendulum
import json
from datetime import datetime
from airflow.models import Variable


def upload_to_blob_storage(date_str, **kwargs):
    ti = kwargs['ti']
    print('ti', ti)
    rslt = ti.xcom_pull(task_ids=f'nx_info_{date_str}')
    print(rslt)

    conn_str = Variable.get('connection_string')
    container_name = "strcontainer"

    blob_service_client = BlobServiceClient.from_connection_string(conn_str)
    container_client = blob_service_client.get_container_client(container_name)

    raw_data = rslt
    filename=f"nx_extract_data-{date_str}.json"

    try:
        blob_client = container_client.get_blob_client(filename)
        data_json = json.dumps(raw_data, indent=4, sort_keys=True, ensure_ascii=False)
        blob_client.upload_blob(data_json, blob_type="BlockBlob")

    except Exception as e:
        return f"An error occureed while uploading to Blob Storage:{str(e)}"


with DAG(
    dag_id='dags_nx_simple_http_operator',
    start_date=pendulum.datetime(2023,3,1,tz='Asia/Seoul'),
    catchup=False,
    schedule_interval='* * * * *'

) as dag:

    start_date = pendulum.datetime(2024, 1, 1, tz='Asia/Seoul')
    end_date = pendulum.datetime(2024, 6, 30, tz='Asia/Seoul')
    current_date = start_date

    while current_date < end_date:

        date_str = current_date.format('YYYY-MM-DD')

        nx_info_task = NxApiToJsonOperator(
            task_id=f'nx_info_{date_str}',
            http_conn_id='nx_api',
            endpoint=f'/maplestory/v1/ranking/overall?date={date_str}',
            dag=dag
        )

        python_task = PythonOperator(
            task_id=f'python_task_{date_str}',
            python_callable=upload_to_blob_storage,
            op_args=[date_str],
            provide_context=True,
            dag=dag
        )

        nx_info_task >> python_task

        current_date = current_date.add(days=1)

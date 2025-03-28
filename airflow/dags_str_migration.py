import time

from azure.storage.blob import BlobServiceClient
import pendulum
import json
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task


def upload_data_to_des(filename, blob_data):

    conn_str = Variable.get('dest_connection_string')
    des_container_name = "descontainer"

    target_service = BlobServiceClient.from_connection_string(conn_str)
    target_container = target_service.get_container_client(des_container_name)
    target_blob_client = target_container.get_blob_client(filename)
    target_blob_client.upload_blob(blob_data, overwrite=True)

    print(f"✅ migrated: {filename}")

@task(task_id='migration_blobs_one_by_one')
def migration_blobs_one_by_one():
    conn_str = Variable.get('connection_string')
    str_container_name = "strcontainer"

    source_service = BlobServiceClient.from_connection_string(conn_str)
    source_container = source_service.get_container_client(str_container_name)

    for blob in source_container.list_blobs():
        if blob.name.endswith(".json"):
            filename = blob.name
            print("filename", filename)

            source_blob_client = source_container.get_blob_client(filename)
            blob_data = source_blob_client.download_blob().readall()

            upload_data_to_des(filename, blob_data)
            print("")
            time.sleep(10)


with DAG(
    dag_id="blob_migration_every_30s",
    description="Migrate JSON files from source to destination Blob Storage every 30 seconds.",
    start_date=pendulum.datetime(2023,3,1,tz='Asia/Seoul'),
    schedule_interval=None,  # 수동 실행
    catchup=False,
    tags=["azure", "migration", "json"],
) as dag:

    migration_blobs_one_by_one()


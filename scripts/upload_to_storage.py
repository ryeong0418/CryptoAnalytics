from azure.storage.blob import BlobServiceClient
import json
from dotenv import load_dotenv
import os


def upload_to_blob_storage(date_str, directory, **kwargs):

    load_dotenv()

    print(date_str,directory)

    ti = kwargs['ti']
    rslt = ti.xcom_pull(task_ids=f"candlestick_daily_data_{date_str}")

    conn_str = os.getenv('CONNECTION_STRING')
    container_name = os.getenv('CONTAINER_NAME')

    blob_service_client = BlobServiceClient.from_connection_string(conn_str)
    container_client = blob_service_client.get_container_client(container_name)

    raw_data = rslt
    filename = f"candlestick_daily_data_{date_str}"
    storage_position = f"{directory}/{filename}"

    try:
        blob_client = container_client.get_blob_client(storage_position)
        data_json = json.dumps(raw_data, indent=4, sort_keys=True, ensure_ascii=False)
        blob_client.upload_blob(data_json, blob_type="BlockBlob")

        return "Upload successful"

    except Exception as e:
        return f"An error occurred while uploading to Blob Storage: {str(e)}"

# def upload_to_blob_storage(market, date_str, conn_str, container_name, data_json, directory,**kwargs):
#     ti = kwargs['ti']
#     rslt = ti.xcom_pull(task_ids=f"candlestick_daily_data_{date_str}")
#
#     filename = f"{market}-{date_str}.json"
#     blob_service_client = BlobServiceClient.from_connection_string(conn_str)
#     container_client = blob_service_client.get_container_client(container_name)
#
#     try:
#         storage_position=f"{directory}/{filename}"
#         blob_clinet = container_client.get_blob_client(storage_position)
#         data_json = json.dumps(data_json, indent=4, sort_keys=True, ensure_ascii=False)
#         blob_clinet.upload_blob(data_json, blob_type='BlockBlob')
#
#     except json.JSONDecodeError as e:
#         print(f"JSON encoding/decoding error: {str(e)}. Please check the input data structure.")
#
#     except Exception as e:
#         print(f"An unexpected error occurred: {str(e)}")
#
#
# if __name__ == "__main__":
#     upload_to_blob_storage()
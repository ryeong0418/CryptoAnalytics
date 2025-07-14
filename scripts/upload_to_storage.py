from scripts.common.utils import SystemUtils
from datetime import datetime, timedelta


def upload_to_blob_storage(market_url, execution_date, **kwargs):

    from azure.storage.blob import BlobServiceClient
    from airflow.models import Variable

    previous_execution_date = (datetime.strptime(execution_date, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')

    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids=f"candlestick_daily_data_{previous_execution_date}")

    conn_str = Variable.get('azure_storage_connection_string')
    container_name = 'analyticscontainer'
    blob_service_client = BlobServiceClient.from_connection_string(conn_str)
    container_client = blob_service_client.get_container_client(container_name)

    market_list = SystemUtils.get_market_list(market_url)

    for market in market_list:
        filename = f'{market}-{previous_execution_date}.json'
        storage_position = f"{market}/{filename}"


        try:
            blob_client = container_client.get_blob_client(storage_position)
            market_data = data.get(market, None)
            blob_client.upload_blob(market_data.encode('utf-8'), blob_type="BlockBlob", overwrite=True)
            print(f"✅ 업로드 완료: {storage_position}")

        except Exception as e:
            print(f"❌ 업로드 실패: {storage_position}")

    return "All uploads attempted"


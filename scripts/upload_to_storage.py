from scripts.common.utils import SystemUtils


def upload_to_blob_storage(market_url, execution_date, **kwargs):
    from azure.storage.blob import BlobServiceClient
    from airflow.models import Variable
    print(execution_date)

    execution_date_str = execution_date
    print('******************************')
    print('execution_date',execution_date_str)
    print('******************************')
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids=f"candlestick_daily_data_{execution_date_str}")
    print('candlestick_daily_data', data)

    conn_str = Variable.get('azure_storage_connection_string')
    container_name = 'candlestick2024'
    blob_service_client = BlobServiceClient.from_connection_string(conn_str)
    container_client = blob_service_client.get_container_client(container_name)

    market_list = SystemUtils.get_market_list(market_url)
    # market_dir = market.replace("-","_") if market else "default"
    # filename = f"{execution_date[:10]}.json"

    for market in market_list:
        filename = f'{market}-{execution_date_str}.json'
        storage_position = f"{market}/{filename}"
        print('storage_position', storage_position)
        # init_blob_path = f"{market}/.init"
        # print('init_blob_path', init_blob_path)
        #
        # try:
        #     blob_client = container_client.get_blob_client(init_blob_path)
        #     if not blob_client.exists():
        #         blob_client.upload_blob(b"", overwrite=True)
        #         print(f"📁 Market 디렉토리 초기화: {market}")
        # except Exception as e:
        #     print(f"❗ Market 디렉토리 초기화 실패: {e}")

        try:
            blob_client = container_client.get_blob_client(storage_position)
            market_data = data.get(market, None)
            blob_client.upload_blob(market_data.encode('utf-8'), blob_type="BlockBlob", overwrite=True)
            print(f"✅ 업로드 완료: {storage_position}")
            return "Upload successful"
        except Exception as e:
            print(f"❌ 업로드 실패: {storage_position}")
            return f"An error occurred while uploading to Blob Storage: {str(e)}"


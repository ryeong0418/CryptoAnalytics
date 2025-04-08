
def upload_to_blob_storage(data, filename, directory, market=None):

    from azure.storage.blob import BlobServiceClient
    import json
    import os

    conn_str = os.getenv('candlestick_storage_connection_string')
    container_name = 'candlestick2024'

    blob_service_client = BlobServiceClient.from_connection_string(conn_str)
    container_client = blob_service_client.get_container_client(container_name)

    market_dir = market.replace("-","_") if market else "default"
    storage_position = f"{directory}/{market_dir}/{filename}"

    init_blob_path = f"{directory}/{market_dir}/.init"
    try:
        blob_client = container_client.get_blob_client(init_blob_path)
        if not blob_client.exists():
            blob_client.upload_blob(b"", overwrite=True)
            print(f"📁 Market 디렉토리 초기화: {market_dir}")
    except Exception as e:
        print(f"❗ Market 디렉토리 초기화 실패: {e}")

    try:
        blob_client = container_client.get_blob_client(storage_position)
        blob_client.upload_blob(data, blob_type="BlockBlob", overwrite=True)

        print(f"✅ 업로드 완료: {storage_position}")
        return "Upload successful"

    except Exception as e:
        print(f"❌ 업로드 실패: {storage_position}")
        return f"An error occurred while uploading to Blob Storage: {str(e)}"


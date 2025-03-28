from azure.storage.blob import BlobServiceClient


def extract_data_from_str():

    conn_str = "DefaultEndpointsProtocol=https;AccountName=strsource;AccountKey=t0TMabhEt5MPVDRxpCXGciW2oC5rnUYqPCHOqwVuN9vbCo+Ndy9Do4EHLKPpAdC0EJ3kBcR67nQZ+AStO/Gilw==;EndpointSuffix=core.windows.net"
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


def upload_data_to_des(filename, blob_data):

    conn_str = "DefaultEndpointsProtocol=https;AccountName=nxpro;AccountKey=DpkKa4WDxfHbD6TbUCyaRDYu1G838cOtDOaaLRZHQ6+a+AGjGOL6grqPpePhvJHT5SRjAvLecsC9+AStRJALvw==;EndpointSuffix=core.windows.net"
    des_container_name = "descontainer"

    target_service = BlobServiceClient.from_connection_string(conn_str)
    target_container = target_service.get_container_client(des_container_name)

    target_blob_client = target_container.get_blob_client(filename)
    target_blob_client.upload_blob(blob_data, overwrite=True)

    print("migration")


if __name__ == "__main__":
    extract_data_from_str()


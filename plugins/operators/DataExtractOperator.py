from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
from pprint import pprint
import logging
import requests
from airflow.models import Variable

class DataExtractOperator(BaseOperator):

    def __init__(self, http_conn_id, endpoint, **kwargs):
        super().__init__(**kwargs)
        self.http_conn_id = http_conn_id
        self.endpoint = endpoint
        self.api_key = Variable.get("apikey_crypto")

    def execute(self, context):

        connection = BaseHook.get_connection(self.http_conn_id)
        logging.info(f"connection:{connection}")

        headers = {}

        result = requests.get()
        raw_data = result.json()
from typing import Any

from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
from pprint import pprint
import logging
import requests
from airflow.models import Variable
from airflow.utils.context import Context


class NxApiToJsonOperator(BaseOperator):

    def __init__(self, http_conn_id, endpoint, **kwargs):
        super().__init__(**kwargs)
        self.http_conn_id = http_conn_id
        self.endpoint = endpoint


    def execute(self, context):
        connection = BaseHook.get_connection(self.http_conn_id)
        logging.info(f"connection:{connection}")
        self.URI = f'{connection.host}{self.endpoint}'

        api_key = Variable.get("apikey_openapi_nx")
        headers = {'x-nxopen-api-key': api_key}

        result = requests.get(self.URI, headers=headers)
        raw_data = result.json()
        pprint(raw_data)

        return raw_data

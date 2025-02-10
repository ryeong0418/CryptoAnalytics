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

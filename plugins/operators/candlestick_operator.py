from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
from pprint import pprint
import logging
import requests
import json
from airflow.models import Variable


class CandleStickOperator(BaseOperator):

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


# class CandleStick:
#     ''''''
#     def __init__(self, http_conn_id, endpoint, **kwargs):
#         self.http_conn_id = http_conn_id
#         self.endpoint = endpoint
#         self.api_key = Variable.get("APIKEY_CRYPTO")
#         self.connection = self.get_connection()
#         logging.info(f"connection:{self.connection}")
#
#
#     def get_connection(self):
#         '''Retrieve the connection object from Airflow BaseHook.'''
#         return BaseHook.get_connection(self.http_conn_id)


class CandleStick:

    def __init__(self):

        self.all_market = 'https://api.upbit.com/v1/market/all'

    def get_market_list(self):
        """
        :return: market_list 출력
        """

        total_market_list= []
        headers = {'accept':'application/json'}
        response = requests.get(self.all_market, headers=headers)
        data = response.json()

        for i in data:
            if i['korean_name'] in ['이더리움', '비트코인', '리플']:
                total_market_list.append(i['market'])

        return total_market_list

    def mapping_market_container(self, data_json, changed_date, market):
        #market list중에서 내가 보고싶은 시장 출력
        '''

        :param data_json:
        :param changed_date:
        :param market:
        :return:

        dictionary mapping을 통해 중복된 코드를 줄이고 간결하게 표시
        '''

        market_to_container={
                                'KRW-BTC': 'historicaldata-krw-btc',
                                'KRW-ETH': 'historicaldata-krw-eth',
                                'BTC-ETH': 'historicaldata-btc-eth',
                                'BTC-XRP': 'historicaldata-btc-xrp',
                                'USDT-BTC': 'historicaldata-usdt-btc',
                                'USDT-ETH': 'historicaldata-usdt-eth',
                                'USDT-XRP': 'historicaldata-usdt-xrp',
                                'KRW-XRP': 'historicaldata-krw-xrp'

                            }

        if market in market_to_container.keys():
            print(market)
            return market

class CandleStick_Monthly(CandleStick):
    ''''''
    def __init__(self):
        super().__init__()


class CandleStick_Daily(CandleStick):
    ''''''
    def __init__(self):
        super().__init__()
        self.candles_days = 'https://api.upbit.com/v1/candles/days'

    def extract_daily_data(self, market_list):

        for market in market_list:
            market_data=[]

            params={
                'market': market,
                'count':1,
                'to':'2024-10-01 00:00:00'

            }

            headers = {'accept':'application/json'}
            response = requests.get(self.candles_days, params=params, headers=headers)

            if response.status_code != 200:
                raise ValueError(f"HTTP Error: {response.status_code} - {response.reason}")

            daily_candle_data = response.json()
            data_json = json.dumps(daily_candle_data, indent=4, sort_keys=True, ensure_ascii=False)
            print(data_json)



class CandleStick_Minutes(CandleStick):
    ''''''

    def __init__(self):
        super().__init__()


class CandleStick_Seconds(CandleStick):
    ''''''

    def __init__(self):
        super().__init__()


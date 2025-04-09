from airflow.models.baseoperator import BaseOperator
from scripts.common.utils import SystemUtils
import requests
import json


class CandleStickDailyOperator(BaseOperator):

    def __init__(self, execution_date:str, **kwargs):
        super().__init__(**kwargs)
        self.execution_date = execution_date
        self.all_market = "https://api.upbit.com/v1/market/all"
        self.market_list = SystemUtils.get_market_list(self.all_market)
        self.candles_days = 'https://api.upbit.com/v1/candles/days'

    def execute(self, context):
        result = {}

        for market in self.market_list:

            params = {
                'market': market,+
                'count':1,
                'to': self.execution_date

            }

            headers = {'accept':'application/json'}
            response = requests.get(self.candles_days, params=params, headers=headers)

            if response.status_code != 200:
                print(f"❌ {market} 실패: {response.status_code}")
                continue

            daily_candle_data = response.json()
            data_json = json.dumps(daily_candle_data, indent=4, sort_keys=True, ensure_ascii=False)

            result[market] = data_json

        return result

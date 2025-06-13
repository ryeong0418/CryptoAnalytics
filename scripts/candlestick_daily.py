from airflow.models.baseoperator import BaseOperator
from scripts.common.utils import SystemUtils
import requests
import json
import time


class CandleStickDailyOperator(BaseOperator):

    def __init__(self, execution_date:str, **kwargs):
        super().__init__(**kwargs)
        self.execution_date = execution_date
        self.all_market = "https://api.upbit.com/v1/market/all"
        self.candles_days = 'https://api.upbit.com/v1/candles/days'

    def execute(self, context):
        result = {}

        market_list = SystemUtils.get_market_list(self.all_market)
        print("마켓 리스트")
        print(market_list)
        print(self.execution_date)

        for market in market_list:

            to_param = f"{self.execution_date}T00:00:00"
            params = {
                'market': market,
                'to': to_param
            }
            headers = {'accept':'application/json'}
            response = requests.get(self.candles_days, params=params, headers=headers)
            print(response)

            if response.status_code != 200:
                print(f"❌ {market} 실패: {response.status_code}")
                continue

            time.sleep(1)
            daily_candle_data = response.json()
            data_json = json.dumps(daily_candle_data, indent=4, sort_keys=True, ensure_ascii=False)
            result[market] = data_json
            print(result[market])

        return result

import requests
import json


class CandleStick():

    def __init__(self):

        self.all_market = "https://api.upbit.com/v1/market/all"
        self.market_list = self.get_market_list()

    def get_market_list(self):

        total_market_list = []
        headers = {'accept':'application/json'}
        response = requests.get(self.all_market, headers=headers)
        data = response.json()

        for i in data:
            if i['market'] in ['KRW-BTC', 'KRW-ETH', 'KRW-XRP']:
                total_market_list.append(i['market'])

        return total_market_list


class CandleStick_Daily(CandleStick):

    def __init__(self):
        super().__init__()
        self.candles_days = 'https://api.upbit.com/v1/candles/days'

    def extract_daily_data(self, execution_date):
        result = {}

        for market in self.market_list:

            params = {
                'market': market,
                'count':1,
                'to': execution_date

            }
            print(market)
            headers = {'accept':'application/json'}
            response = requests.get(self.candles_days, params=params, headers=headers)

            if response.status_code != 200:
                print(f"❌ {market} 실패: {response.status_code}")
                continue

            daily_candle_data = response.json()
            data_json = json.dumps(daily_candle_data, indent=4, sort_keys=True, ensure_ascii=False)

            result[market] = data_json

        return result

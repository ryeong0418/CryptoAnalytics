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
            if i['korean_name'] in ['이더리움', '비트코인', '엑스알피']:
                total_market_list.append(i['market'])
        return total_market_list


class CandleStick_Daily(CandleStick):

    def __init__(self):

        super().__init__()
        self.candles_days = 'https://api.upbit.com/v1/candles/days'

    def extract_daily_data(self, execution_date):

        for market in self.market_list:
            print(market)

            params = {
                'market': market,
                'count':2,
                'to':execution_date

            }

            headers = {'accept':'application/json'}
            response = requests.get(self.candles_days, params=params, headers=headers)

            if response.status_code != 200:
                raise ValueError(f"HTTP Error: {response.status_code} - {response.reason}")

            daily_candle_data = response.json()
            data_json = json.dumps(daily_candle_data, indent=4, sort_keys=True, ensure_ascii=False)

            return data_json


candlestick_daily = CandleStick_Daily()
candlestick_daily.extract_daily_data()
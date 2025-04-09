import requests
import json


total_market_list = []
all_market = "https://api.upbit.com/v1/market/all"
candles_days = 'https://api.upbit.com/v1/candles/days'
headers = {'accept': 'application/json'}
response = requests.get(all_market, headers=headers)
data = response.json()
print(data)

for i in data:
    if i['market'] in ['KRW-BTC', 'KRW-ETH', 'KRW-XRP']:
        total_market_list.append(i['market'])

print(total_market_list)
result={}
for market in total_market_list:

    params = {
        'market': market,
        'to': '2024-05-01T00:00:00'
    }
    headers = {'accept': 'application/json'}
    response = requests.get(candles_days, params=params, headers=headers)

    if response.status_code != 200:
        print(f"❌ {market} 실패: {response.status_code}")
        continue

    daily_candle_data = response.json()
    print(daily_candle_data)
    data_json = json.dumps(daily_candle_data, indent=4, sort_keys=True, ensure_ascii=False)
    print(data_json)
    result[market] = data_json
    print(result[market])
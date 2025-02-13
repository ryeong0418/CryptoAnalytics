import requests


all_market = 'https://api.upbit.com/v1/market/all'
total_market_list = []
headers = {'accept': 'application/json'}
response = requests.get(all_market, headers=headers)
data = response.json()
print(data)
for i in data:
    if i['korean_name'] in ['이더리움', '비트코인', '엑스알피']:
        total_market_list.append(i['market'])




market_to_container = {
    'KRW-BTC': 'historicaldata-krw-btc',
    'KRW-ETH': 'historicaldata-krw-eth',
    'BTC-ETH': 'historicaldata-btc-eth',
    'BTC-XRP': 'historicaldata-btc-xrp',
    'USDT-BTC': 'historicaldata-usdt-btc',
    'USDT-ETH': 'historicaldata-usdt-eth',
    'USDT-XRP': 'historicaldata-usdt-xrp',
    'KRW-XRP': 'historicaldata-krw-xrp'

}

for market in total_market_list:
    if market in market_to_container.keys():
        print(market)


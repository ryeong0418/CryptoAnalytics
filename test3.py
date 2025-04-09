import requests

total_market_list = []
headers = {'accept': 'application/json'}
response = requests.get("https://api.upbit.com/v1/market/all", headers=headers)
data = response.json()
print(data)

for i in data:
    if i['market'] in ['KRW-BTC','KRW-ETH','KRW-XRP']:
        print(i['market'])

# for i in data:
#     if i['korean_name'] in ['이더리움', '비트코인', '엑스알피']:
#         total_market_list.append(i['market'])
#
# print(total_market_list)
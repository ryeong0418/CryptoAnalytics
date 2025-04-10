import requests


class SystemUtils:

    @staticmethod
    def get_market_list(all_market):

        total_market_list = []
        headers = {'accept':'application/json'}
        response = requests.get(all_market, headers=headers)
        data = response.json()

        for i in data:
            if isinstance(i,dict) and i.get('market') in ['KRW-BTC', 'KRW-ETH', 'KRW-XRP']:
                total_market_list.append(i['market'])

        return total_market_list
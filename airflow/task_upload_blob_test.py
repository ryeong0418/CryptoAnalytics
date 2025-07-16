import os
import sys
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

# 사용자 정의 모듈 경로 추가
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
# from scripts.upload_to_storage import upload_to_blob_storage
from scripts.common.utils import SystemUtils
import requests
import json
import time

def get_market_list(**context):
    execution_date = context["ds"]
    print(f"실행일자: {execution_date}")

    all_market = "https://api.upbit.com/v1/market/all"
    market_list = SystemUtils.get_market_list(all_market)
    print("마켓 리스트")
    print(market_list)

    return market_list

    # for market in market_list:
    #
    #     to_param = f"{self.execution_date}T00:00:00"
    #     params = {
    #         'market': market,
    #         'to': to_param
    #     }
    #     headers = {'accept': 'application/json'}
    #     response = requests.get(self.candles_days, params=params, headers=headers)
    #     print(response)
    #
    #     if response.status_code != 200:
    #         print(f"❌ {market} 실패: {response.status_code}")
    #         continue
    #
    #     time.sleep(1)
    #     daily_candle_data = response.json()
    #     data_json = json.dumps(daily_candle_data, indent=4, sort_keys=True, ensure_ascii=False)
    #     result[market] = data_json
    #     print(result[market])
    #
    # return result

with DAG(
    dag_id="task_upload_blob_test",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    end_date=pendulum.datetime(2024, 1, 10, tz="Asia/Seoul"),
    max_active_runs=1,
    tags=["upload", "candlestick"],
) as dag:

    candlestick_task = PythonOperator(
        task_id="candlestick_task",
        python_callable=get_market_list
    )



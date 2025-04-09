from airflow.decorators import dag, task
from scripts.fetch_and_upload import fetch_and_upload_by_date
from datetime import datetime, timedelta
import pendulum


# ✅ 날짜 리스트 생성 함수
def generate_date_list(start: datetime, end: datetime):
    date_list = []
    current = start
    while current <= end:
        date_list.append(current.strftime('%Y-%m-%dT00:00:00'))
        current += timedelta(days=1)
    return date_list

# ✅ DAG 정의
@dag(
    dag_id = 'dags_historical_candlestick_data',
    start_date=pendulum.datetime(2024, 1, 1, tz='Asia/Seoul'),
    schedule_interval='@once',
    catchup=False,
    tags=['candlestick', 'backfill'],
    max_active_tasks=20  # 동시에 실행할 태스크 수
)

def backfill_upbit_dag():

    @task
    def run_by_date(date_str:str):
        print(f"📅 실행 날짜: {date_str}")
        fetch_and_upload_by_date(date_str)

    start_date=datetime(2024,1,1)
    end_date=datetime(2024,12,31)
    date_list = generate_date_list(start_date, end_date)

    run_by_date.expand(date_str=date_list)


dag = backfill_upbit_dag()
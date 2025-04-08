from scripts.candlestick_data import CandleStick_Daily
from datetime import datetime, timedelta

def fetch_and_upload_upbit_data_full_year():
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 1, 10)

    cs = CandleStick_Daily()
    current_date = start_date

    while current_date <= end_date:
        execution_date = current_date.strftime('%Y-%m-%dT00:00:00')
        print(f'수집중:{execution_date}')

        try:
            data = cs.extract_daily_data(execution_date)

            # TODO: 저장 또는 업로드 처리
            filename = f"{current_date.strftime('%Y-%m-%d')}.json"
            print('filename', filename)
            upload_to_blob_storage(data, filename, directory='candlestick-storage')

            print(f'{execution_date} 데이터 저장 완료')

        except Exception as e:
            print(f'{execution_date} 데이터 수집 실패')

        current_date += timedelta(days=1)

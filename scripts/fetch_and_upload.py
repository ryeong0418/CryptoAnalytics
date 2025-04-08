from scripts.candlestick_data import CandleStick_Daily
from scripts.upload_to_storage import upload_to_blob_storage


def fetch_and_upload_by_date(execution_date: str):

    cs = CandleStick_Daily()
    all_market_data = cs.extract_daily_data(execution_date)

    for market, data in all_market_data.items():

        filename = f"{execution_date[:10]}.json"
        upload_to_blob_storage(
            data=data,
            filename=filename,
            directory=f"{market}"
        )
        print(f"✅ {execution_date} {market} 업로드 완료")


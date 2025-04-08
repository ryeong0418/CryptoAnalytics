from datetime import datetime, timedelta

date_time = (datetime.today() - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
formatted_date_time = date_time.strftime('%Y-%m-%d %H:%M:%S')

print(formatted_date_time)




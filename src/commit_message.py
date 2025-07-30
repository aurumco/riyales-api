from datetime import datetime
import pytz
import jdatetime

tehran_time = datetime.now(pytz.timezone("Asia/Tehran"))
shamsi_date = jdatetime.datetime.fromgregorian(datetime=tehran_time).strftime("%Y-%m-%d")
time_str = tehran_time.strftime("%H:%M:%S")

commit_message = f"Auto: Data update {shamsi_date.replace('-', '/')} {time_str}"
print(commit_message)

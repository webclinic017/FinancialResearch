import datetime
import pandas as pd
from typing import Union

def time2str(date: Union[str, datetime.date, datetime.datetime]) -> str:
    if isinstance(date, (datetime.datetime, datetime.date)):
        date = date.strftime(r'%Y-%m-%d')
    return date

def str2time(date: Union[str, datetime.date, datetime.datetime]) -> datetime.datetime:
    if isinstance(date, (str, datetime.date)):
        date = pd.to_datetime(date)
    return date
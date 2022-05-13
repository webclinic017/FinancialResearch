import datetime
import pandas as pd
import pandasquant as pq
import sqlalchemy as sql


def check(table: str, date_col: str):
    # check whether the table exists
    table_status = stockdb.execute("SELECT name FROM sqlite_master"
        " WHERE type='table' AND name='%s'" % table).fetchall()
    if table_status:
        # table exists, check the date diffrence
        sql = f"select distinct({date_col}) from {table}"
        dates = pd.read_sql(sql, stockdb, index_col=date_col, parse_dates=True).index
        dates = pd.to_datetime(dates)
        diff = tables[table]['check_date'].difference(dates)
        return diff
    else:
        # table not exists, return the total date
        return tables[table]['check_date']

today = datetime.datetime.today() if datetime.datetime.today().hour >= 22 \
    else datetime.datetime.today() - datetime.timedelta(days=1)
stockdb = sql.create_engine('sqlite:///./data.nosync/stock.db')

# check whether the trade_date table exists
trade_date_table = stockdb.execute("SELECT name FROM sqlite_master"
    " WHERE type='table' AND name='trade_date'").fetchall()
    
if not trade_date_table:
    # table not exists
    pq.Api.trade_date(start='20070101', end='20231231').databaser.\
        to_sql('trade_date', stockdb, index=False, on_duplicate=True)
    
current_trade_dates = pd.read_sql(f"select trading_date from trade_date where trading_date <= '{today}'", 
    stockdb, index_col="trading_date", parse_dates='trading_date').index
current_report_dates = pd.date_range(start='2007-01-01', end=today, freq='Q')

tables = {     
    "trade_date": {
        "func": pq.Api.trade_date,
        "date_col": "trading_date",
        "check_date": current_trade_dates,
    },
    "plate_info": {
        "func": pq.Api.plate_info,
        "date_col": "date",
        "check_date": current_trade_dates,
    }, 
    "market_daily": {
        "func": pq.Api.market_daily,
        "date_col": "trading_date",
        "check_date": current_trade_dates,
    }, 
    "index_market_daily": {
        "func": pq.Api.index_market_daily,
        "date_col": "trading_date",
        "check_date": current_trade_dates,
    }, 
    "derivative_indicator": {
        "func": pq.Api.derivative_indicator,
        "date_col": "trading_date",
        "check_date": current_trade_dates,
    }, 
    "pit_financial": {
        "func": pq.Api.pit_financial,
        "date_col": "trading_date",
        "check_date": current_trade_dates,
    }, 
    "balance_sheet": {
        "func": pq.Api.balance_sheet,
        "date_col": "report_period",
        "check_date": current_report_dates,
    }, 
    "cashflow_sheet": {
        "func": pq.Api.cashflow_sheet,
        "date_col": "report_period",
        "check_date": current_report_dates,
    }, 
    "income_sheet": {
        "func": pq.Api.income_sheet,
        "date_col": "report_period",
        "check_date": current_report_dates,
    },
    "index_weight": {
        "func": pq.Api.index_weight,
        "date_col": "date",
        "check_date": current_trade_dates[current_trade_dates >= '20100104'],
    },
    "financial_indicator": {
        "func": pq.Api.financial_indicator,
        "date_col": "report_period",
        "check_date": current_report_dates,
    },
    "intensity_trend": {
        "func": pq.Api.intensity_trend,
        "date_col": "trading_date",
        "check_date": current_trade_dates,
    },
}

for table, conf in tables.items():
    print(f'[*] Getting latest data for {table} ...')
    diff = check(table, conf['date_col'])
    print(f'[*] {len(diff)} rows need to be updated')
    for day in diff:
        print(f'[*] Updating {day} in {table} ...')
        conf['func'](start=day, end=day).databaser.to_sql(
            table=table, database=stockdb, index=True, on_duplicate="update")
    print(f'[+] Update {table} success')

print(f'[+] All Tables are up to date now')

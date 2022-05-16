import pandas as pd
import numpy as np
import pandasquant as pq
from factor.tools.config import config
from pandasquant.tools.common import str2time

def get_factor_data(factor_name, date, *args, **kwargs):
    date = pq.item2list(date)
    data = []
    for dt in date:
        print(f'[*] Getting factor data for {factor_name} on {dt}')
        data.append(config[factor_name]['instance'](dt, *args, **kwargs))
    return pd.concat(data, axis=0)

def get_forward_return(date, period):
    date = pq.item2list(date)
    data = []
    for dt in date:
        dt = str2time(dt)
        print(f'[*] Getting forward return on {dt}')
        next_dt = pq.Stock.nearby_n_trade_date(dt, 1)
        period_dt = pq.Stock.nearby_n_trade_date(dt, period)
        price_next_dt = pq.Stock.market_daily(next_dt,
            next_dt, fields='adj_open').droplevel(0)
        price_period_dt = pq.Stock.market_daily(period_dt, 
            period_dt, fields='adj_close').droplevel(0)
        ret = (price_period_dt.adj_close - 
            price_next_dt.adj_open) / price_next_dt.adj_open
        ret.index = pd.MultiIndex.from_product([[dt], ret.index],
            names = ['datetime', 'asset'])
        data.append(ret)
    return pd.concat(data, axis=0)


if __name__ == "__main__":
    print(get_forward_return('2022-05-11', 2))
    
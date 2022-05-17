import pandas as pd
import pandasquant as pq
from factor.define.base import FactorBase


def get_factor_data(factor: FactorBase, date: list):
    date = pq.item2list(date)
    data = []
    for dt in date:
        print(f'[*] Getting factor {factor} on {dt}')
        data.append(factor(dt))
    return pd.concat(data, axis=0).astype('float64')

def get_forward_return(date: list, period: str):
    date = pq.item2list(date)
    data = []
    for dt in date:
        dt = pq.str2time(dt)
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
    return pd.concat(data, axis=0).astype('float64')

def get_industry_mapping(date: list):
    date = pq.item2list(date)
    data = []
    for dt in date:
        print(f'[*] Getting industry mapping on {dt}')
        data.append(pq.Stock.plate_info(dt, dt, 
            fields='citi_industry_name1').citi_industry_name1)
    return pd.concat(data, axis=0)


if __name__ == "__main__":
    print(get_forward_return('20100129', 21))
    
import pandas as pd
import pandasquant as pq
import akshare as ak


@pq.RedisCache(rediscon=pq.REDIS, prefix='viewer', expire=3600 * 24)
def fund(code: str, start: str, end: str):
    data = ak.fund_open_fund_info_em(code, indicator='单位净值走势')
    data['净值日期'] = pd.to_datetime(data['净值日期'])
    data['open'] = data['单位净值']
    data['high'] = data['单位净值']
    data['low'] = data['单位净值']
    data['close'] = data['单位净值']
    data['volume'] = data['单位净值']
    data = data.drop(['单位净值', '日增长率'], axis=1)
    data = data.set_index('净值日期')
    data = data.loc[start:end]
    return data

@pq.RedisCache(rediscon=pq.REDIS, prefix='viewer', expire=3600 * 24)
def stock(code: str, start: str, end: str):
    return pq.Api.market_daily(start=start, end=end, 
        fields=['n_open', 'n_high', 'n_low', 'n_close', 'n_volume'], 
        conditions='c_windCode=%s' % code).rename(columns=
        lambda x: pq.hump2snake(x.split('.')[-1])).droplevel(1) 

__all__ = [
    'fund',
    'stock',
]


if __name__ == "__main__":
    code = '000001'
    start = '2019-01-01'
    end = '2020-01-01'
    data = fund(code, start, end)
    print(data)
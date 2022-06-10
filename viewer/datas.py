import akshare as ak
import pandas as pd
import pandasquant as pq
import backtrader as bt


@pq.RedisCache(prefix='viewer')
def stockfeedapi(code: str, start: str, end: str, fromdate: str = None, todate: str = None):
    data = pq.Api.market_daily(start, end, fields=['n_adjOpen', 'n_adjHigh', 'n_adjLow', 'n_adjClose', 'n_volume'],
        conditions='c_windCode=%s' % code).droplevel(1).rename(columns={
            'adj_open': 'open',
            'adj_high': 'high',
            'adj_low': 'low',
            'adj_close': 'close',
        })
    fromdate = pq.str2time(fromdate) if fromdate else data.index[0]
    todate = pq.str2time(todate) if todate else data.index[-1]
    feed = bt.feeds.PandasData(dataname=data, fromdate=fromdate, todate=todate)
    return feed

@pq.RedisCache(prefix='viewer')
def etffeedsina(code: str, fromdate: str = None, todate: str = None):
    data = ak.fund_etf_hist_sina(symbol=code)
    data = data.rename(columns={'date': 'datetime'})
    data['datetime'] = pd.to_datetime(data['datetime'])
    data = data.set_index('datetime')
    fromdate = pq.str2time(fromdate) if fromdate else data.index[0]
    todate = pq.str2time(todate) if todate else data.index[-1]
    feed = bt.feeds.PandasData(dataname=data, fromdate=fromdate, todate=todate)
    return feed


if __name__ == '__main__':
    print(etffeedsina('sh510300'))
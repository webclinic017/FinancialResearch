from factor.define.pricevolume import Momentum
import pandasquant as pq
import matplotlib.pyplot as plt
from factor.tools import (
    factor_analysis,
    get_factor_data, 
    get_forward_return, 
    get_industry_mapping)


factors = [Momentum(20)]
plt.rcParams['font.family'] = ['Songti SC']

start_date = '20210101'
end_date = '20211130'
benchmark_code = '000001.SH'
forward_period = 1

trade_dates =  pq.Stock.trade_date(start_date, end_date,
    fields='trading_date').iloc[:, 0].dropna().to_list()
forward = pq.Stock.nearby_n_trade_date(end_date, 2 * forward_period)
forward_dates = pq.Stock.trade_date(end_date, forward,
    fields='trading_date').iloc[:, 0].dropna().to_list()
forward_return = get_forward_return(trade_dates + forward_dates, forward_period)

industry_grouper = get_industry_mapping(trade_dates)
benchmark = pq.Stock.index_market_daily(code=benchmark_code, start=trade_dates[0], 
    end=trade_dates[-1], fields='close').droplevel(1).close
benchmark = benchmark / benchmark.iloc[0]
benchmark.name = 'benchmark'

for factor in factors:
    factor_data = get_factor_data(factor, trade_dates)
    factor_analysis(factor_data, forward_return, industry_grouper, benchmark,
        q=5, datapath=f'result.nosync/table/{factor}.xlsx', imagepath=f'result.nosync/image/{factor}.png')

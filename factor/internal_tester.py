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
end_date = '20211230'
benchmark_code = '000001.SH'
"""解释: 调仓频率由forward_period和freq共同决定。
   例如forward_period = 1, freq = 'monthly'; 调仓频率为一个月
   例如forward_period = 2, freq = 'monthly'; 调仓频率为两个月
   例如forward_period = 1, freq = 'daily'; 调仓频率为一日
   例如forward_period = 5, freq = 'daily'; 调仓频率为一周
"""
forward_period = 1
freq = 'monthly' # support daily or monthly

forward_return, trade_dates = get_forward_return(start_date, end_date, forward_period, freq)

industry_grouper = get_industry_mapping(trade_dates)
benchmark = pq.Stock.index_market_daily(code=benchmark_code, start=trade_dates[0], 
    end=trade_dates[-1], fields='close').droplevel(1).close
benchmark = benchmark / benchmark.iloc[0]
benchmark.name = 'benchmark'

for factor in factors:
    factor_data = get_factor_data(factor, trade_dates)
    factor_analysis(factor_data, forward_return, industry_grouper, benchmark,
        q=5, datapath=f'result.nosync/table/{factor}.xlsx', imagepath=f'result.nosync/image/{factor}.png')

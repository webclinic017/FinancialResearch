import pandas as pd
import pandasquant as pq
from factor.tools.common import get_factor_data, get_forward_return

trade_dates =  pq.Stock.trade_date('20150101', '20220401',
    fields='trading_month').trading_month.dropna().to_list()
forward_return = get_forward_return(trade_dates, 20)
benchmark = pq.Stock.index_market_daily(code='000300.SH', start='20150101', 
    end='20220401', fields='close').droplevel(1)
benchmark = benchmark / benchmark.iloc[0]

for factor in ['roegq', 'ocfgq', 'salesgq']:
    factor_data = get_factor_data(factor, trade_dates)
    ic = factor_data.describer.ic(forward_return)
    quantiles = factor_data.iloc[:, 0].groupby(level=0).apply(pd.qcut, q=5, labels=False) + 1
    profit = forward_return.relocator.profit(grouper=quantiles)

    with pq.Gallery(2, 1) as g:
        ic.drawer.draw('bar', ax=g.axes[0, 0])
        profit.drawer.draw('bar', ax=g.axes[1, 0])
        (profit + 1).groupby(level=1).cumprod().drawer.draw('line', ax=g.axes[1, 0].twinx())
        benchmark.drawer.draw('line', indicator='close', ax=g.axes[1, 0].twinx(),
            label='benchmark', color='black')

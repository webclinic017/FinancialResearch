import pandas as pd
import pandasquant as pq
from factor.tools import (
    get_factor_data, get_forward_return, get_industry_mapping,
    factors)

start_date = '20100101'
end_date = '20220501'
benchmark_code = '000300.SH'
forward_period = 21

trade_dates =  pq.Stock.trade_date(start_date, end_date,
    fields='trading_month').trading_month.dropna().to_list()
forward_date = pq.Stock.nearby_n_trade_date(end_date, forward_period)
forward_dates = pq.Stock.trade_date(end_date, forward_date,
    fields='trading_month').trading_month.dropna().to_list()
forward_return = get_forward_return(trade_dates + forward_dates, forward_period)
present_return = forward_return.groupby(level=1).shift(1)
present_return.loc[present_return.index.get_level_values(0)[0]] = 0

industry_grouper = get_industry_mapping(trade_dates)
benchmark = pq.Stock.index_market_daily(code=benchmark_code, start=trade_dates[0], 
    end=trade_dates[-1], fields='close').droplevel(1).close
benchmark = benchmark / benchmark.iloc[0]
benchmark.name = 'benchmark'

for factor in factors:
    factor_data = get_factor_data(factor, trade_dates)
    barra_reg = pd.concat([pd.get_dummies(industry_grouper).iloc[:, 1:], factor_data], 
        axis=1).regressor.ols(forward_return)
    ic = factor_data.describer.ic(forward_return)
    quantiles = factor_data.iloc[:, 0].groupby(level=0).apply(pd.qcut, 
        q=5, labels=False, duplicates='drop') + 1
    profit = present_return.relocator.profit(grouper=quantiles)
    cumprofit = pd.concat([(profit + 1).groupby(level=1).cumprod().unstack(),
        benchmark], axis=1).dropna()

    with pq.Gallery(3, 1, path=f'factor/result.nosync/{factor}.png', show=False) as g:
        barra_reg.drawer.draw('bar', asset=str(factor), indicator='coef', ax=g.axes[0, 0])
        barra_reg.drawer.draw('line', asset=str(factor), indicator='t', 
            ax=g.axes[0, 0].twinx(), color='#aa1111', title='barra-regression')
        ic.drawer.draw('bar', ax=g.axes[1, 0])
        ic.rolling(12).mean().drawer.draw('line', ax=g.axes[1, 0].twinx(), 
            color='#1899B3', title='ic-test')
        profit.drawer.draw('bar', ax=g.axes[2, 0])
        cumprofit.drawer.draw('line', ax=g.axes[2, 0].twinx(), title='layering-test')

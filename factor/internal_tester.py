import pandas as pd
import pandasquant as pq
from factor.define.growth import *
from factor.define.leverage import *
from factor.define.pricevolume import *
from factor.define.quality import *
from factor.define.size import *
from factor.define.technical import *
from factor.define.turnover import *
from factor.define.valuation import *
from factor.define.volatility import *
from factor.tools import (
    get_factor_data, 
    get_forward_return, 
    get_industry_mapping
    )


factors = [
    # Std(6),
    # Std(20),
    # Turnover(6), 
    Amplitude(20),
    # BiasTurnover(2, 6),
    # Momentum(2),
    # Momentum(6),
    # Momentum(11),
    WeightedMomentum(20),
    # WeightedMomentum(2),
    # WeightedMomentum(6),
    # ExpWeightedMomentum(2),
    # ExpWeightedMomentum(6),
    # ExpWeightedMomentum(11),
]

# [
#     SalesGQ(),
#     ProfitGQ(),
#     OcfGQ(),
#     RoeGQ(),
#     FinancialLeverage(),
#     DebtEquityRatio(),
#     CashRatio(),
#     CurrentRatio(),
#     HAlpha(60),
#     HBeta(60),
#     Momentum(20),
#     WeightedMomentum(20),
#     ExpWeightedMomentum(20),
#     LogPrice(),
#     RoeQ(),
#     RoeTTM(),
#     RoaQ(),
#     RoaTTM(),
#     GrossProfitMarginQ(),
#     GrossProfitMarginTTM(),
#     ProfitMarginQ(),
#     ProfitMarginTTM(),
#     AssetTurnoverQ(),
#     AssetTurnoverTTM(),
#     OperationCashflowRatioQ(),
#     OperationCashflowRatioTTM(),
#     Capital(),
#     Macd(),
#     Turnover(20),
#     BiasTurnover(20, 126),
#     Ep(),
#     Epcut(),
#     Bp(),
#     Sp(),
#     Ncfp(),
#     Ocfp(),
#     Dp(),
#     Gpe(),
#     Std(20),
# ]

start_date = '20210101'
end_date = '20211130'
benchmark_code = '000001.SH'
forward_period = 1

trade_dates =  pq.Stock.trade_date(start_date, end_date,
    fields='trading_date').iloc[:, 0].dropna().to_list()
forward_date = pq.Stock.nearby_n_trade_date(end_date, forward_period)
forward_next_date = pq.Stock.nearby_n_trade_date(end_date, 1)
forward_dates = pq.Stock.trade_date(forward_next_date, forward_date,
    fields='trading_date').iloc[:, 0].dropna().to_list()
forward_return = get_forward_return(trade_dates + forward_dates, forward_period)

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
    quantiles = factor_data.iloc[:, 0].groupby(level=0).apply(pd.qcut, q=5, labels=False) + 1
    profit = forward_return.relocator.profit(grouper=quantiles)
    cumprofit = (profit + 1).groupby(level=1).cumprod().unstack().shift(1).fillna(1)
    cumprofit = pd.concat([cumprofit, benchmark], axis=1).dropna()

    with pq.Gallery(3, 1, path=f'factor/result.nosync/{factor}.png', show=False) as g:
        barra_reg.drawer.draw('bar', asset=str(factor), indicator='coef', ax=g.axes[0, 0])
        barra_reg.drawer.draw('line', asset=str(factor), indicator='t', 
            ax=g.axes[0, 0].twinx(), color='#aa1111', title='barra-regression')
        ic.drawer.draw('bar', ax=g.axes[1, 0])
        g.axes[1, 0].hlines(y=0.03, xmin=g.axes[1, 0].get_xlim()[0], xmax=g.axes[1, 0].get_xlim()[1],
            color='#aa1111', linestyles='dashed')
        g.axes[1, 0].hlines(y=-0.03, xmin=g.axes[1, 0].get_xlim()[0], xmax=g.axes[1, 0].get_xlim()[1],
            color='#aa1111', linestyles='dashed')
        ic.rolling(12).mean().drawer.draw('line', ax=g.axes[1, 0], 
            color='#1899B3', title='ic-test')
        profit.drawer.draw('bar', ax=g.axes[2, 0])
        cumprofit.drawer.draw('line', ax=g.axes[2, 0].twinx(), title='layering-test')

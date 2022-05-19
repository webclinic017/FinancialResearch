import pandas as pd
import pandasquant as pq
from functools import wraps
from factor.define.base import FactorBase


def get_factor_data(factor: FactorBase, date: list):
    date = pq.item2list(date)
    data = []
    for dt in date:
        print(f'[*] Getting factor {factor} on {dt}')
        data.append(factor(dt))
    return pd.concat(data, axis=0).astype('float64')

def get_forward_return(date: list, period: int):
    date = pq.item2list(date)
    data = []
    for dt in date:
        dt = pq.str2time(dt)
        print(f'[*] Getting forward return on {dt}')
        next_dt = pq.Stock.nearby_n_trade_date(dt, 1)
        period_dt = pq.Stock.nearby_n_trade_date(next_dt, period)
        price_next_dt = pq.Stock.market_daily(next_dt,
            next_dt, fields='adj_open').droplevel(0)
        price_period_dt = pq.Stock.market_daily(period_dt, 
            period_dt, fields='adj_open').droplevel(0)
        ret = (price_period_dt.iloc[:, 0] - 
            price_next_dt.iloc[:, 0]) / price_next_dt.iloc[:, 0]
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

def process_factor(func: ..., name: str = None, *args, **kwargs):
    @wraps(func)
    def wrapper(*args, **kwargs):
        factor = func(*args, **kwargs)
        if isinstance(factor, pd.DataFrame):
            factor = factor.iloc[:, 0]
        factor.name = name or 'factor'
        return factor
    return wrapper

def factor_test(factor: pd.Series, forward_return: pd.Series, grouper: pd.Series,
    benchmark: pd.Series = None, q: int = 5, path: str = None, show: bool = True):
    reg_data = pd.concat([pd.get_dummies(grouper).iloc[:, 1:], factor], axis=1)
    reg_res = reg_data.regressor.ols(y=forward_return)
    ic = factor.describer.ic(forward_return)
    quantiles = factor.groupby(level=0).apply(pd.qcut, q=q, labels=False) + 1
    profit = forward_return.relocator.profit(grouper=quantiles)
    cumprofit = (profit + 1).groupby(level=1).cumprod().unstack().shift(1).fillna(1)
    
    if benchmark is not None:
        cumprofit = pd.concat([cumprofit, benchmark], axis=1).dropna()

    if path is not None:
        with pd.ExcelWriter(path) as writer:
            reg_res.to_excel(writer, sheet_name='regression-test-result')
            ic.to_excel(writer, sheet_name='ic-test-result')
            cumprofit.to_excel(writer, sheet_name='layering-test-result')
    
    if show:
        with pq.Gallery(3, 1, path=f'factor/result.nosync/{factor}.png', show=False) as g:
            reg_res.drawer.draw('bar', asset=factor.name, indicator='coef', ax=g.axes[0, 0])
            reg_res.drawer.draw('line', asset=str(factor), indicator='t', 
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


if __name__ == "__main__":
    print(get_forward_return('20100129', 21))
    
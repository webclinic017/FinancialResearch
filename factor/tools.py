import pandas as pd
import pandasquant as pq
import numpy as np
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

def process_factor(name: str = None, *args, **kwargs):
    def decorate(func):
        @wraps(func)
        def wrapper():
            factor = func(*args, **kwargs)
            if isinstance(factor, pd.DataFrame):
                factor = factor.iloc[:, 0]
            factor.name = name or 'factor'
            return factor
        return wrapper
    return decorate

def factor_analysis(factor: pd.Series, forward_return: pd.Series, 
    grouper: pd.Series = None, benchmark: pd.Series = None, q: int = 5, 
    commission: float = 0.001, commision_type: str = 'both',
    datapath: str = None, show: bool = True, savedata: list = ['reg', 'ic', 'layering', 'turnover'],
    imagepath: str = None, boxplot_period: 'int | str' = -1, scatter_period: 'int | str' = -1):
    '''Factor test pipeline
    ----------------------

    factor: pd.Series, factor to test
    forward_return: pd.Series, forward return of factor
    grouper: pd.Series, grouper of factor
    benchmark: pd.Series, benchmark of factor
    ic_grouped: bool, whether to calculate IC in certain groups
    q: int, q-quantile
    commission: float, commission rate
    commission_type: str, commission type, 'both', 'buy', 'sell'
    datapath: str, path to save result, must be excel file
    show: bool, whether to show result
    savedata: list, data to save, ['reg', 'ic', 'layering', 'turnover']
    imagepath: str, path to save image, must be png file
    '''
    if datapath is not None and not datapath.endswith('.xlsx'):
        raise ValueError('path must be an excel file')
    
    # regression test
    if grouper is not None:
        reg_data = pd.concat([pd.get_dummies(grouper).iloc[:, 1:], factor], axis=1)
    else:
        reg_data = factor
    reg_res = reg_data.regressor.ols(y=forward_return)
    # ic test
    ic = factor.describer.ic(forward_return)
    if grouper:
        ic_group = factor.describer.ic(forward_return, grouper=grouper)
    # layering test
    quantiles = factor.groupby(level=0).apply(pd.qcut, q=q, labels=False) + 1
    weight = pd.Series(np.ones_like(quantiles), index=quantiles.index)
    profit = weight.groupby(quantiles).apply(lambda x: 
        x.relocator.profit(forward_return)).swaplevel().sort_index()
    turnover = weight.groupby(quantiles).apply(lambda x: x.relocator.turnover(side=commision_type)
        ).swaplevel().sort_index()
    turnover_commission = turnover * commission
    profit = profit - turnover_commission
    cumprofit = (profit + 1).groupby(level=1).cumprod().unstack().shift(1).fillna(1)
    # gathering data together
    data = pd.concat([factor, forward_return, grouper], axis=1)

    if benchmark is not None:
        benchmark_ret = benchmark / benchmark.iloc[0]
        cumprofit = pd.concat([cumprofit, benchmark_ret], axis=1).dropna()

    if show:
        with pq.Gallery(6, 1, show=show, path=imagepath) as g:
            cross_section_period = data.dropna().index.get_level_values(0).unique()

            # first boxplot on a given period
            if isinstance(boxplot_period, int):
                boxplot_data = data.dropna().loc[(cross_section_period[boxplot_period], 
                    slice(None)), [factor.name, grouper.name]]
            elif isinstance(boxplot_period, str):
                boxplot_data = data.dropna().loc[(boxplot_period, slice(None)),
                    [factor.name, grouper.name]]   
            boxplot_data.drawer.draw('boxplot', by=grouper.name, ax=g.axes[0, 0], 
                whis=(5, 95), datetime=boxplot_data.index.get_level_values(0)[0])
            g.axes[0, 0].set_title('boxplot on latest date')
            g.axes[0, 0].set_xlabel('')

            # second scatter plot on a given period
            if isinstance(scatter_period, int):
                scatter_data = pd.concat([factor, forward_return], axis=1).loc[
                    (cross_section_period[scatter_period], slice(None)), :]
            elif isinstance(scatter_period, str):
                scatter_data = pd.concat([factor, forward_return], axis=1).loc[
                    (scatter_period, slice(None)), :]
            scatter_data.drawer.draw('scatter', x=factor.name, y=forward_return.name, 
                ax=g.axes[1, 0], datetime=scatter_data.index.get_level_values(0)[0], s=1)

            # third regression plot
            reg_res.drawer.draw('bar', asset=factor.name, indicator='coef', ax=g.axes[2, 0])
            reg_res.drawer.draw('line', asset=factor.name, indicator='t', 
                ax=g.axes[2, 0].twinx(), color='#aa1111', title='barra regression')

            # fourth IC plot
            ic.drawer.draw('bar', ax=g.axes[3, 0])
            g.axes[3, 0].hlines(y=0.03, xmin=g.axes[3, 0].get_xlim()[0], 
                xmax=g.axes[3, 0].get_xlim()[1], color='#aa1111', linestyles='dashed')
            g.axes[3, 0].hlines(y=-0.03, xmin=g.axes[3, 0].get_xlim()[0], 
                xmax=g.axes[3, 0].get_xlim()[1], color='#aa1111', linestyles='dashed')
            ic.rolling(12).mean().drawer.draw('line', 
                ax=g.axes[3, 0], color='#1899B3', title='ic test')

            # fifth layering plot
            profit.drawer.draw('bar', ax=g.axes[4, 0])
            cumprofit.drawer.draw('line', ax=g.axes[4, 0].twinx(), title='layering test')

            # sixth turnover plot
            turnover.unstack().drawer.draw('line', ax=g.axes[5, 0], title='layering turnover')

    if datapath is not None:
        with pd.ExcelWriter(datapath) as writer:
            if 'reg' in savedata:
                reg_res.index.names = ['datetime', 'x_variables']
                reg_res.reset_index().to_excel(writer, sheet_name='regression-test-result', index=False)
            if 'ic' in savedata:
                ic.name = 'datetime'
                if grouper:
                    ic_group.index.names = ['datetime', 'group']
                    ic_group.reset_index().to_excel(writer, sheet_name='ic-group-test-result', index=False)
                ic.reset_index().to_excel(writer, sheet_name='ic-test-result', index=False)
            if 'layering' in savedata:
                profit_data = pd.concat([profit, cumprofit.stack()], axis=1)
                profit_data.index.names = ['datetime', 'quantiles']
                profit_data.columns = ['profit', 'cumprofit']
                profit_data.reset_index().to_excel(writer, sheet_name='layering-test-result', index=False)
            if 'data' in savedata:
                data.index.names = ['datetime', 'asset']
                data.reset_index().to_excel(writer, sheet_name='data', index=False)
            if 'turnover' in savedata:
                turnover.index.names = ['datetime', 'quantiles']
                turnover.reset_index().to_excel(writer, sheet_name='turnover', index=False)


if __name__ == "__main__":
    from factor.define.valuation import Ep
    open_price = pq.Stock.market_daily('20210101', '20210131', fields='open')
    forward_return = open_price.groupby(
        level=1).shift(-1) / open_price.groupby(level=1).shift(-2) - 1
    industry = pq.Stock.plate_info(
        '20210101', '20210131', fields='citi_industry_name1')
    factor_data = get_factor_data(Ep(), pq.Stock.trade_date('20210101', '20210131', 
        fields='trading_date').trading_date.tolist())
    factor_analysis(factor_data.ep, forward_return.open, 
        industry.citi_industry_name1, q=5, ic_grouped=True, commission=0)

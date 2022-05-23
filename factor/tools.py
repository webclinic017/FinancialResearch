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
    pure_factor_data = pd.concat(data, axis=0).astype('float64').iloc[:,0]
    return pure_factor_data

def get_forward_return(start_date, end_date, period: int, freq: str):
    freq_setting = {'daily':1, 'monthly':21}
    trade_dates =  pq.Stock.trade_date(start_date, end_date,
                fields='trading_date').iloc[:, 0].dropna().to_list()
    next_date = pq.Stock.nearby_n_trade_date(end_date, 1)
    forward_date = pq.Stock.nearby_n_trade_date(next_date, period * freq_setting[freq])
    forward_dates = pq.Stock.trade_date(end_date, forward_date, fields='trading_date').iloc[:, 0].dropna().to_list()
    date = trade_dates + forward_dates
    date = pq.item2list(date)
    date = pd.DataFrame(date, columns=['orignal_date'])
    shift_1_day = []
    for d in date['orignal_date'].to_list():
        shift_1_day.append(pq.Stock.nearby_n_trade_date(d, 1))
    date = pd.concat([date, pd.Series(shift_1_day).rename('shift_1_day')], axis=1)
    date['shift_1_period'] = date['shift_1_day'].shift(-period * freq_setting[freq])
    date = date.dropna()
    date = date.iloc[[ i for i in range(0, len(date), period * freq_setting[freq])], :].reset_index(drop=True)
    data = []
    for _, row in date.iterrows():
        dt = pq.str2time(row['orignal_date'])
        price_buy_date = pq.Stock.market_daily(row['shift_1_day'],
            row['shift_1_day'], fields='adj_open').droplevel(0)
        price_sell_date = pq.Stock.market_daily(row['shift_1_period'],
            row['shift_1_period'], fields='adj_open').droplevel(0)
        if price_buy_date.empty or price_sell_date.empty:
            print(f'[!] Cannot get forward return on {dt}')
            break
        else:
            print(f'[*] Getting forward return on {dt}')
        ret = price_sell_date.iloc[:, 0] / price_buy_date.iloc[:, 0] - 1
        ret.index = pd.MultiIndex.from_product([[row['orignal_date']], ret.index],
            names = ['datetime', 'asset'])
        data.append(ret)
    data = pd.concat(data, axis=0).astype('float64')
    trade_dates = data.index.get_level_values(0).drop_duplicates().to_list()
    return data, trade_dates

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

# def factor_analysis(factor: pd.Series, forward_return: pd.Series, 
#     grouper: pd.Series = None, benchmark: pd.Series = None, q: int = 5, 
#     commission: float = 0.001, commision_type: str = 'both',
#     datapath: str = None, show: bool = True, savedata: list = ['reg', 'ic', 'layering', 'turnover'],
#     imagepath: str = None, boxplot_period: 'int | str' = -1, scatter_period: 'int | str' = -1):
#     '''Factor test pipeline
#     ----------------------

#     factor: pd.Series, factor to test
#     forward_return: pd.Series, forward return of factor
#     grouper: pd.Series, grouper of factor
#     benchmark: pd.Series, benchmark of factor
#     ic_grouped: bool, whether to calculate IC in certain groups
#     q: int, q-quantile
#     commission: float, commission rate
#     commission_type: str, commission type, 'both', 'buy', 'sell'
#     datapath: str, path to save result, must be excel file
#     show: bool, whether to show result
#     savedata: list, data to save, ['reg', 'ic', 'layering', 'turnover']
#     imagepath: str, path to save image, must be png file
#     '''
#     if datapath is not None and not datapath.endswith('.xlsx'):
#         raise ValueError('path must be an excel file')
    
#     # regression test
#     if grouper is not None:
#         reg_data = pd.concat([pd.get_dummies(grouper).iloc[:, 1:], factor], axis=1)
#     else:
#         reg_data = factor
#     reg_res = reg_data.regressor.ols(y=forward_return)
#     # ic test
#     ic = factor.describer.ic(forward_return)
#     if grouper is not None:
#         ic_group = factor.describer.ic(forward_return, grouper=grouper)
#     # layering test
#     quantiles = factor.groupby(level=0).apply(pd.qcut, q=q, labels=False) + 1
#     weight = pd.Series(np.ones_like(quantiles), index=quantiles.index)
#     profit = weight.groupby(quantiles).apply(lambda x: 
#         x.relocator.profit(forward_return)).swaplevel().sort_index()
#     turnover = weight.groupby(quantiles).apply(lambda x: x.relocator.turnover(side=commision_type)
#         ).swaplevel().sort_index()
#     turnover_commission = turnover * commission
#     profit = profit - turnover_commission
#     cumprofit = (profit + 1).groupby(level=1).cumprod().unstack().shift(1).fillna(1)
#     # gathering data together
#     data = pd.concat([factor, forward_return, grouper], axis=1)

#     if benchmark is not None:
#         benchmark_ret = benchmark / benchmark.iloc[0]
#         cumprofit = pd.concat([cumprofit, benchmark_ret], axis=1).dropna()

#     if show:
#         with pq.Gallery(6, 1, show=show, path=imagepath) as g:
#             cross_section_period = data.dropna().index.get_level_values(0).unique()

#             # first boxplot on a given period
#             if isinstance(boxplot_period, int):
#                 boxplot_data = data.dropna().loc[(cross_section_period[boxplot_period], 
#                     slice(None)), [factor.name, grouper.name]]
#             elif isinstance(boxplot_period, str):
#                 boxplot_data = data.dropna().loc[(boxplot_period, slice(None)),
#                     [factor.name, grouper.name]]   
#             boxplot_data.drawer.draw('boxplot', by=grouper.name, ax=g.axes[0, 0], 
#                 whis=(5, 95), datetime=boxplot_data.index.get_level_values(0)[0])
#             g.axes[0, 0].set_title('boxplot on latest date')
#             g.axes[0, 0].set_xlabel('')

#             # second scatter plot on a given period
#             if isinstance(scatter_period, int):
#                 scatter_data = pd.concat([factor, forward_return], axis=1).loc[
#                     (cross_section_period[scatter_period], slice(None)), :]
#             elif isinstance(scatter_period, str):
#                 scatter_data = pd.concat([factor, forward_return], axis=1).loc[
#                     (scatter_period, slice(None)), :]
#             scatter_data.drawer.draw('scatter', x=factor.name, y=forward_return.name, 
#                 ax=g.axes[1, 0], datetime=scatter_data.index.get_level_values(0)[0], s=1)

#             # third regression plot
#             reg_res.drawer.draw('bar', asset=factor.name, indicator='coef', ax=g.axes[2, 0])
#             reg_res.drawer.draw('line', asset=factor.name, indicator='t', 
#                 ax=g.axes[2, 0].twinx(), color='#aa1111', title='barra regression')

#             # fourth IC plot
#             ic.drawer.draw('bar', ax=g.axes[3, 0])
#             g.axes[3, 0].hlines(y=0.03, xmin=g.axes[3, 0].get_xlim()[0], 
#                 xmax=g.axes[3, 0].get_xlim()[1], color='#aa1111', linestyles='dashed')
#             g.axes[3, 0].hlines(y=-0.03, xmin=g.axes[3, 0].get_xlim()[0], 
#                 xmax=g.axes[3, 0].get_xlim()[1], color='#aa1111', linestyles='dashed')
#             ic.rolling(12).mean().drawer.draw('line', 
#                 ax=g.axes[3, 0], color='#1899B3', title='ic test')

#             # fifth layering plot
#             profit.drawer.draw('bar', ax=g.axes[4, 0])
#             cumprofit.drawer.draw('line', ax=g.axes[4, 0].twinx(), title='layering test')

#             # sixth turnover plot
#             turnover.unstack().drawer.draw('line', ax=g.axes[5, 0], title='layering turnover')

#     if datapath is not None:
#         with pd.ExcelWriter(datapath) as writer:
#             if 'reg' in savedata:
#                 reg_res.index.names = ['datetime', 'x_variables']
#                 reg_res.reset_index().to_excel(writer, sheet_name='regression-test-result', index=False)
#             if 'ic' in savedata:
#                 ic.name = 'datetime'
#                 if grouper is not None:
#                     ic_group.index.names = ['datetime', 'group']
#                     ic_group.reset_index().to_excel(writer, sheet_name='ic-group-test-result', index=False)
#                 ic.reset_index().to_excel(writer, sheet_name='ic-test-result', index=False)
#             if 'layering' in savedata:
#                 profit_data = pd.concat([profit, cumprofit.stack()], axis=1)
#                 profit_data.index.names = ['datetime', 'quantiles']
#                 profit_data.columns = ['profit', 'cumprofit']
#                 profit_data.reset_index().to_excel(writer, sheet_name='layering-test-result', index=False)
#             if 'data' in savedata:
#                 data.index.names = ['datetime', 'asset']
#                 data.reset_index().to_excel(writer, sheet_name='data', index=False)
#             if 'turnover' in savedata:
#                 turnover.index.names = ['datetime', 'quantiles']
#                 turnover.reset_index().to_excel(writer, sheet_name='turnover', index=False)

class PureFactorAnalysis(object):

    def __init__(self, 
                factor_data, 
                forward_return, 
                imagepath, 
                datapath) -> None:
        self.factor_data = factor_data
        self.forward_return = forward_return
        self.imagepath = imagepath
        self.datapath = datapath
        self.show = False
        self.layer_number = 5
        self.commision_type = 'both'
        self.commission = 0.001
        
     
    def cross_sectional_test(self, canvas, boxplot_period = -1, grouper: pd.Series = None):
        data = pd.concat([self.factor_data, self.forward_return, grouper], axis=1)
        cross_section_period = data.dropna().index.get_level_values(0).unique()
        
        if isinstance(boxplot_period, int):
            boxplot_data = data.dropna().loc[(cross_section_period[boxplot_period], 
                slice(None)), [self.factor_data.name, grouper.name]]
        elif isinstance(boxplot_period, str):
            boxplot_data = data.dropna().loc[(boxplot_period, slice(None)),
                [self.factor_data.name, grouper.name]]   
        boxplot_data.drawer.draw('boxplot', by=grouper.name, ax=canvas, 
            whis=(5, 95), datetime=boxplot_data.index.get_level_values(0)[0])
        canvas.set_title('boxplot on latest date')
        canvas.set_xlabel('')
        
    # regression test
    def barra_test(self, canvas, writer = None, grouper: pd.Series = None):
        if grouper is not None:
            reg_data = pd.concat([pd.get_dummies(grouper).iloc[:, 1:], self.factor_data], axis=1)
        else:
            reg_data = self.factor_data.copy()
        barra_res = reg_data.regressor.ols(y=self.forward_return)
        
        if grouper is not None:
            pass
        else:
            barra_res.drawer.draw('bar', asset=self.factor_data.name, indicator='coef', ax=canvas)
            barra_res.drawer.draw('line', asset=self.factor_data.name, indicator='t', 
                ax=canvas.twinx(), color='#aa1111', title='barra regression')
        
        if writer is not None:
            if grouper is not None:
                pass
            else:
                barra_res.index.names = ['datetime', 'x_variables']
                barra_res.reset_index().to_excel(writer, sheet_name='regression-test-result', index=False)
        return barra_res
    
    def ic_test(self, canvas, writer = None, grouper: pd.Series = None):
        # ic
        if grouper is not None:
            ic = self.factor_data.describer.ic(self.forward_return, grouper=grouper)
        else:
            ic = self.factor_data.describer.ic(self.forward_return)
            
        # how to draw
        if grouper is not None:
            pass
        else:
            ic.drawer.draw('bar', ax=canvas)
            canvas.hlines(y=0.03, xmin=canvas.get_xlim()[0], 
                xmax=canvas.get_xlim()[1], color='#aa1111', linestyles='dashed')
            canvas.hlines(y=-0.03, xmin=canvas.get_xlim()[0], 
                xmax=canvas.get_xlim()[1], color='#aa1111', linestyles='dashed')
            ic.rolling(12).mean().drawer.draw('line', 
                ax=canvas, color='#1899B3', title='ic test')
            
        # how to save
        if writer is not None:
            ic.name = 'datetime'
            if grouper is not None:
                ic.index.names = ['datetime', 'group']
                ic.reset_index().to_excel(writer, sheet_name='ic-group-test-result', index=False)
            else:
                ic.reset_index().to_excel(writer, sheet_name='ic-test-result', index=False)
                
        return ic
        
    def layering_test(self, canvas, writer = None, grouper: pd.Series = None, benchmark: pd.Series = None):
        
        if grouper is not None:
            pass
        else:
            quantiles = self.factor_data.groupby(level=0).apply(pd.qcut, q=self.layer_number, labels=False) + 1
            weight = pd.Series(np.ones_like(quantiles), index=quantiles.index)
            profit = weight.groupby(quantiles).apply(lambda x: 
                x.relocator.profit(self.forward_return)).swaplevel().sort_index()
            turnover = weight.groupby(quantiles).apply(lambda x: x.relocator.turnover(side=self.commision_type)
                ).swaplevel().sort_index()
            turnover_commission = turnover * self.commission
            profit = profit - turnover_commission
            cumprofit = (profit + 1).groupby(level=1).cumprod().unstack().shift(1).fillna(1)
            
            self.turnover = turnover
            
            if benchmark is not None:
                benchmark_ret = benchmark / benchmark.iloc[0]
                cumprofit = pd.concat([cumprofit, benchmark_ret], axis=1).dropna()
        
        if grouper is not None:
            pass
        else:
            profit.drawer.draw('bar', ax=canvas)
            cumprofit.drawer.draw('line', ax=canvas.twinx(), title='layering test')
            
        if writer is not None:
            if grouper is not None:
                pass
            else:
                profit_data = pd.concat([profit, cumprofit.stack()], axis=1)
                profit_data.index.names = ['datetime', 'quantiles']
                profit_data.columns = ['profit', 'cumprofit']
                profit_data.reset_index().to_excel(writer, sheet_name='layering-test-result', index=False)
        
    def turnover_test(self, canvas, grouper: pd.Series = None):
        if grouper is None:
            self.turnover.unstack().drawer.draw('line', ax=canvas, title='layering turnover')
            
    def test_scatter(self, canvas, scatter_period: 'int | str' = -1, grouper: pd.Series = None):
        data = pd.concat([self.factor_data, self.forward_return, grouper], axis=1)
        cross_section_period = data.dropna().index.get_level_values(0).unique()
        if isinstance(scatter_period, int):
            scatter_data = pd.concat([self.factor_data, self.forward_return], axis=1).loc[
                (cross_section_period[scatter_period], slice(None)), :]
        elif isinstance(scatter_period, str):
            scatter_data = pd.concat([self.factor_data, forward_return], axis=1).loc[
                (scatter_period, slice(None)), :]
        scatter_data.drawer.draw('scatter', x=self.factor_data.name, y=self.forward_return.name, 
            ax=canvas, datetime=scatter_data.index.get_level_values(0)[0], s=1)
        
    def __call__(self, grouper: pd.Series = None, benchmark: pd.Series = None):
        if grouper is not None:
            draw_number = 6
        else:
            draw_number = 4
        with(
                pq.Gallery(draw_number, 1, show=self.show, path=self.imagepath) as g,
                pd.ExcelWriter(self.datapath) as w
            ):
            self.barra_test(g.axes[0,0],w, grouper=grouper)
            self.ic_test(g.axes[1,0], w, grouper=grouper)
            self.layering_test(g.axes[2,0], w, grouper=grouper, benchmark=benchmark)
            self.turnover_test(g.axes[3,0], grouper=grouper)
            if grouper is not None:
                self.cross_sectional_test(g.axes[4,0], grouper=grouper)
                self.test_scatter(g.axes[5,0],  grouper=grouper)
        
        
    

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
    # price_1_priod = pq.Stock.market_daily('2023-01-04',
    #         '2023-01-04', fields='adj_open').droplevel(0)
    # print(price_1_priod)

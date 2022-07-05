import sys
import pandasquant
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from functools import wraps


class Factor:
    def __init__(self, name: str,
                 pool: 'list | pd.Index | pd.MultiIndex' = None,
                 deextreme: str = 'mad',
                 standardize: str = 'zscore',
                 fillna: str = 'mean',
                 grouper = None,
                 *args, **kwargs):
        self.name = name
        self.pool = pool
        self.deextreme = deextreme
        self.standardize = standardize
        self.fillna = fillna
        self.grouper = grouper
        self.args = args
        self.kwargs = kwargs

    def filter_pool(self) -> 'pd.Series | pd.DataFrame':
        if self.pool is None:
            return self.factor
        
        elif isinstance(self.pool, pd.MultiIndex):
            stocks = self.pool.index.intersection(self.factor.index)
            return self.factor.loc[stocks]

        elif isinstance(self.pool, pd.Index):
            stocks = self.pool.index.intersection(self.factor.index.levels[1])
            return self.factor.loc[stocks]
        
        elif isinstance(self.pool, list):
            stocks = pd.Index(stocks)
            stocks = stocks.index.intersection(self.factor.index.levels[1])
            return self.factor.loc[stocks]
    
    def preprocess(self):
        factor = self.factor.preprocessor.deextreme(self.deextreme, self.grouper)
        factor = factor.preprocessor.standarize(self.standardize, self.grouper)
        factor = factor.preprocessor.fillna(self.fillna, self.grouper)
        return factor
    
    def postprocess(self):
        if isinstance(self.factor, pd.DataFrame) and isinstance(self.factor.index, pd.MultiIndex):
            if self.factor.columns.size != 1:
                raise ValueError('factor must be a single columned dataframe')
            factor = self.factor.iloc[:, 0]
        
        elif isinstance(self.factor, pd.DataFrame) and isinstance(self.factor.index, pd.DatetimeIndex):
            factor = self.factor.stack()
        
        elif isinstance(self.factor, pd.Series):
            factor = self.factor

        factor.index.names = ['datetime', 'asset']
        factor.name = self.name
        return factor
    
    def __call__(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            self.factor = func(*args, **kwargs)
            self.factor = self.preprocess()
            self.factor = self.filter_pool()
            self.factor = self.postprocess()
            return self.factor
        return wrapper

def single_factor_analysis(factor_data: pd.Series, forward_return: 'pd.Series | pd.DataFrame',
                           grouper: 'pd.Series | pd.DataFrame | dict' = None, 
                           benchmark: pd.Series = None, q: int = 5, commission: float = 0.001, 
                           commission_type: str = 'both', plot_period: 'int | str' = -1, 
                           data_path: str = None, image_path: str = None, show: bool = True):
    if isinstance(forward_return, pd.DataFrame):
        forward_return = forward_return.stack()
        forward_return.name = 'forward'

    if isinstance(grouper, pd.DataFrame):
        grouper = grouper.stack()
        grouper.name = 'grouper'

    # slice the common part of data
    common_index = factor_data.index.intersection(forward_return.index)
    if grouper is not None:
        common_index = common_index.intersection(grouper.index)
    
    factor_data = factor_data.loc[common_index]
    forward_return = forward_return.loc[common_index]
    if grouper is not None:
        grouper = grouper.loc[common_index]

    if sys.platform == 'linux':
        plt.rcParams['font.family'] = ['DejaVu Serif']
    elif sys.platform == 'darwin':
        plt.rcParams['font.family'] = ['Songti SC']
    data_writer = pd.ExcelWriter(data_path) if data_path is not None else None
    _, axes = plt.subplots(7, 1, figsize=(12, 7 * 8))
    cross_section_test(factor_data, forward_return, grouper, 
                        plot_period=plot_period, data_writer=data_writer, 
                        boxplot_ax=axes[0], scatter_ax=axes[1], 
                        hist_ax=axes[2])
    if grouper is not None:
        barra_test(factor_data, forward_return, grouper,
                    data_writer=data_writer, barra_ax=axes[3])
                
    ic_test(factor_data, forward_return, grouper,
            data_writer=data_writer, ic_ax=axes[4])
            
    layering_test(factor_data, forward_return, q=q, 
                  commission=commission, commission_type=commission_type,
                  benchmark=benchmark, data_writer=data_writer, 
                  layering_ax=axes[5], turnover_ax=axes[6])
    if image_path is not None:
        plt.savefig(image_path)
    if show:
        plt.show()
    if data_writer is not None:
        data_writer.close()

def cross_section_test(factor_data: pd.Series, forward_return: pd.Series,
                       grouper: pd.Series = None, plot_period: 'int | str' = -1,
                       data_writer: pd.ExcelWriter = None, scatter_ax: plt.Axes = None,
                       boxplot_ax: plt.Axes = None, hist_ax: plt.Axes = None) -> None:
    """"""
    concated_data = pd.concat([factor_data, forward_return, grouper], axis=1, join='inner')
    datatime_index = concated_data.dropna().index.get_level_values(0).unique()
    if isinstance(plot_period, int):
        plot_period = datatime_index[plot_period]

    if data_writer is not None:
        factor_data.loc[plot_period].to_excel(data_writer, sheet_name=f'cross_section_data')
    if boxplot_ax is not None and grouper is not None:
        concated_data.drawer.draw('box', datetime=plot_period, whis=(5, 95),
            by=grouper.name, ax=boxplot_ax, indicator=[factor_data.name, grouper.name])
    if scatter_ax is not None:
        concated_data.drawer.draw('scatter', datetime=plot_period,
            x=factor_data.name, y=forward_return.name, ax=scatter_ax, s=1)
    if hist_ax is not None:
        if grouper is not None:
            for group in grouper.dropna().unique():
                group_cs = concated_data.loc[plot_period].loc[
                    (concated_data[grouper.name] == group).loc[plot_period], factor_data.name]
                if group_cs.empty:
                    continue
                group_cs.drawer.draw('hist', bins=80, ax=hist_ax, label=group, indicator=factor_data.name, alpha=0.7)
        else:
            concated_data.loc[plot_period].drawer.draw('hist', ax=hist_ax, 
                indicator=factor_data.name, bins=80, alpha=0.7)
        hist_ax.legend()

def barra_test(factor_data: pd.Series, forward_return: pd.Series,
               grouper: pd.Series, data_writer: pd.ExcelWriter = None,
               barra_ax: plt.Axes = None) -> None:
    grouper_dummies = pd.get_dummies(grouper).iloc[:, 1:]
    barra_data = pd.concat([grouper_dummies, factor_data, forward_return], axis=1)
    barra_result = barra_data.regressor.ols(
        x_col=barra_data.columns[:-1].tolist(), y_col=barra_data.columns[-1])
    if data_writer is not None:
        barra_result.filer.to_multisheet_excel(data_writer, perspective='indicator')
    if barra_ax is not None:
        barra_result.drawer.draw('bar', ax=barra_ax, asset=factor_data.name, 
            indicator='coef', title='barra regression')
        barra_result.drawer.draw('line', color='#aa1111', 
            ax=barra_ax.twinx(), asset=factor_data.name, indicator='t')

def ic_test(factor_data: pd.Series, forward_return: pd.Series,
            grouper: pd.Series = None, data_writer: pd.ExcelWriter = None,
            ic_ax: plt.Axes = None) -> None:
    ic = factor_data.describer.ic(forward_return)
    if grouper is not None:
        ic_grouped = factor_data.describer.ic(forward=forward_return, grouper=grouper)
        ic_grouped = ic_grouped.loc[ic_grouped.index.get_level_values(1) != 'nan']
    if data_writer is not None:
        ic.to_excel(data_writer, sheet_name='ic test result')
        if grouper is not None:
            ic_grouped.filer.to_multisheet_excel(data_writer, perspective='asset')
    if ic_ax is not None:
        ic.drawer.draw('bar', ax=ic_ax)
        ic.rolling(12).mean().drawer.draw('line', ax=ic_ax, title='IC test')
        ic_ax.hlines(y=0.03, xmin=ic_ax.get_xlim()[0], 
            xmax=ic_ax.get_xlim()[1], color='#aa3333', linestyle='--')
        ic_ax.hlines(y=-0.03, xmin=ic_ax.get_xlim()[0], 
            xmax=ic_ax.get_xlim()[1], color='#aa3333', linestyle='--')

def layering_test(factor_data: pd.Series, forward_return: pd.Series, q: int = 5,
                  commission_type: str = 'both', commission: float = 0.001,
                  benchmark: pd.Series = None, data_writer: pd.ExcelWriter = None,
                  layering_ax: plt.Axes = None, turnover_ax: plt.Axes = None) -> None:
    # TODO: finish layering within group
    quantiles = factor_data.groupby(level=0).apply(pd.qcut, q=q, labels=False) + 1
    weight = pd.Series(np.ones_like(quantiles), index=quantiles.index)
    profit = weight.groupby(quantiles).apply(
        lambda x: x.relocator.profit(forward_return)).swaplevel().sort_index()
    turnover = weight.groupby(quantiles).apply(
        lambda x: x.relocator.turnover(side=commission_type)).swaplevel().sort_index()
    profit = profit - turnover * commission
    profit = profit.groupby(level=1).shift(1).fillna(0)
    cumprofit = (profit + 1).groupby(level=1).cumprod().unstack().shift(1).fillna(1)

    if benchmark is not None:
        benchmark_ret = benchmark / benchmark.iloc[0]
        cumprofit = pd.concat([cumprofit, benchmark_ret], axis=1).dropna()

    if data_writer is not None:
        profit_data = pd.concat([profit, cumprofit.stack()], axis=1)
        profit_data.index.names = ['datetime', 'quantiles']
        profit_data.columns = ['profit', 'cumprofit']
        profit_data.filer.to_multisheet_excel(
            data_writer, perspective='indicator')

    if layering_ax is not None:
        profit.drawer.draw('bar', ax=layering_ax)
        cumprofit.drawer.draw('line', ax=layering_ax.twinx(), title='layering test')
        layering_ax.twinx().hlines(y=1, xmin=layering_ax.get_xlim()[0], 
            xmax=layering_ax.get_xlim()[1], color='grey', linestyle='--')

    if turnover_ax is not None:
        turnover.unstack().drawer.draw('line', ax=turnover_ax, title='turnover')


if __name__ == "__main__":
    pass

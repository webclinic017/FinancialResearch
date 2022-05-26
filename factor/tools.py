import pandas as pd
import pandasquant as pq
import numpy as np
import matplotlib.pyplot as plt
from functools import wraps
from factor.define.base import FactorBase


def get_factor_data(factor: FactorBase, date: list):
    date = pq.item2list(date)
    data = []
    for dt in date:
        print(f'[*] Getting factor {factor} on {dt}')
        data.append(factor(dt))
    pure_factor_data = pd.concat(data, axis=0).astype('float64').iloc[:, 0]
    return pure_factor_data


def get_forward_return(start_date, end_date, period: int, freq: str):
    freq_setting = {'daily': 1, 'monthly': 21}
    trade_dates = pq.Stock.trade_date(start_date, end_date,
                                      fields='trading_date').iloc[:, 0].dropna().to_list()
    next_date = pq.Stock.nearby_n_trade_date(end_date, 1)
    forward_date = pq.Stock.nearby_n_trade_date(
        next_date, period * freq_setting[freq])
    forward_dates = pq.Stock.trade_date(
        end_date, forward_date, fields='trading_date').iloc[:, 0].dropna().to_list()
    date = trade_dates + forward_dates
    date = pq.item2list(date)
    date = pd.DataFrame(date, columns=['orignal_date'])
    shift_1_day = []
    for d in date['orignal_date'].to_list():
        shift_1_day.append(pq.Stock.nearby_n_trade_date(d, 1))
    date = pd.concat(
        [date, pd.Series(shift_1_day).rename('shift_1_day')], axis=1)
    date['shift_1_period'] = date['shift_1_day'].shift(
        -period * freq_setting[freq])
    date = date.dropna()
    date = date.iloc[[i for i in range(
        0, len(date), period * freq_setting[freq])], :].reset_index(drop=True)
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
                                               names=['datetime', 'asset'])
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


def single_factor_analysis(factor_data: pd.Series, forward_return: pd.Series,
                           grouper: pd.Series = None, benchmark: pd.Series = None,
                           q: int = 5, commission: float = 0.001, commission_type: str = 'both',
                           plot_period: 'int | str' = -1, data_path: str = None,
                           image_path: str = None, show: bool = True):
    plt.rcParams['font.family'] = ['Songti SC']
    data_writer = pd.ExcelWriter(data_path) if data_path is not None else None
    with pq.Gallery(7, 1, show=show, path=image_path, xaxis_keep_mask=[1, 0, 0, 0, 0, 0, 0]) as g:
        cross_section_test(factor_data, forward_return, grouper, 
                           plot_period=plot_period, data_writer=data_writer, 
                           boxplot_ax=g.axes[0, 0], scatter_ax=g.axes[1, 0], 
                           hist_ax=g.axes[2, 0])
        barra_test(factor_data, forward_return, grouper,
                   data_writer=data_writer, barra_ax=g.axes[3, 0])
                   
        ic_test(factor_data, forward_return, grouper,
                data_writer=data_writer, ic_ax=g.axes[4, 0])
                
        layering_test(factor_data, forward_return, q=q, 
                      commission=commission, commission_type=commission_type,
                      benchmark=benchmark, data_writer=data_writer, 
                      layering_ax=g.axes[5, 0], turnover_ax=g.axes[6, 0])
    data_writer.close()


def cross_section_test(factor_data: pd.Series, forward_return: pd.Series,
                       grouper: pd.Series = None, plot_period: 'int | str' = -1,
                       data_writer: pd.ExcelWriter = None, scatter_ax: plt.Axes = None,
                       boxplot_ax: plt.Axes = None, hist_ax: plt.Axes = None) -> None:
    if grouper is not None:
        concated_data = pd.concat([factor_data, forward_return, grouper], axis=1)
    else:
        concated_data = pd.concat([factor_data, forward_return], axis=1)

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
                concated_data.loc[plot_period].\
                    loc[(concated_data[grouper.name] == group).loc[plot_period], factor_data.name].\
                    drawer.draw('hist', bins=80, ax=hist_ax, label=group, indicator=factor_data.name, alpha=0.7)
                hist_ax.legend()
        else:
            concated_data[plot_period].drawer.draw('hist', ax=hist_ax, 
                indicator=factor_data.name, bins=80, alpha=0.7)


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
        ic.drawer.draw('bar', ax=ic_ax, label='ic')
        ic.rolling(12).mean().drawer.draw('line', ax=ic_ax, label='ic_rolling12', title='IC test')


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

    if turnover_ax is not None:
        turnover.unstack().drawer.draw('line', ax=turnover_ax, title='turnover')


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

    def cross_sectional_test(self, canvas, boxplot_period=-1, grouper: pd.Series = None):
        data = pd.concat(
            [self.factor_data, self.forward_return, grouper], axis=1)
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
    def barra_test(self, canvas, writer=None, grouper: pd.Series = None):
        if grouper is not None:
            reg_data = pd.concat(
                [pd.get_dummies(grouper).iloc[:, 1:], self.factor_data], axis=1)
        else:
            reg_data = self.factor_data.copy()
        barra_res = reg_data.regressor.ols(y=self.forward_return)

        if grouper is not None:
            pass
        else:
            barra_res.drawer.draw(
                'bar', asset=self.factor_data.name, indicator='coef', ax=canvas)
            barra_res.drawer.draw('line', asset=self.factor_data.name, indicator='t',
                                  ax=canvas.twinx(), color='#aa1111', title='barra regression')

        if writer is not None:
            if grouper is not None:
                pass
            else:
                barra_res.index.names = ['datetime', 'x_variables']
                barra_res.reset_index().to_excel(
                    writer, sheet_name='regression-test-result', index=False)
        return barra_res

    def ic_test(self, canvas, writer=None, grouper: pd.Series = None):
        # ic
        if grouper is not None:
            ic = self.factor_data.describer.ic(
                self.forward_return, grouper=grouper)
        else:
            ic = self.factor_data.describer.ic(self.forward_return)

        # how to draw
        if grouper is not None:
            groupers = [pd.Grouper(level=1)]
            ic = ic.groupby(groupers).mean()
            ic.drawer.draw('bar', ax=canvas)
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
            if grouper is not None:
                ic.index.names = ['group']
                ic.reset_index().to_excel(writer, sheet_name='ic-group-test-result', index=False)
            else:
                ic.reset_index().to_excel(writer, sheet_name='ic-test-result', index=False)

        return ic

    def layering_test(self, canvas, writer=None, grouper: pd.Series = None, benchmark: pd.Series = None):

        if grouper is not None:
            pass
        else:
            quantiles = self.factor_data.groupby(level=0).apply(
                pd.qcut, q=self.layer_number, labels=False) + 1
            weight = pd.Series(np.ones_like(quantiles), index=quantiles.index)
            profit = weight.groupby(quantiles).apply(lambda x:
                                                     x.relocator.profit(self.forward_return)).swaplevel().sort_index()
            turnover = weight.groupby(quantiles).apply(lambda x: x.relocator.turnover(side=self.commision_type)
                                                       ).swaplevel().sort_index()
            turnover_commission = turnover * self.commission
            profit = profit - turnover_commission
            cumprofit = (
                profit + 1).groupby(level=1).cumprod().unstack().shift(1).fillna(1)

            self.turnover = turnover

            if benchmark is not None:
                benchmark_ret = benchmark / benchmark.iloc[0]
                cumprofit = pd.concat(
                    [cumprofit, benchmark_ret], axis=1).dropna()

        if grouper is not None:
            pass
        else:
            profit.drawer.draw('bar', ax=canvas)
            cumprofit.drawer.draw(
                'line', ax=canvas.twinx(), title='layering test')

        if writer is not None:
            if grouper is not None:
                pass
            else:
                profit_data = pd.concat([profit, cumprofit.stack()], axis=1)
                profit_data.index.names = ['datetime', 'quantiles']
                profit_data.columns = ['profit', 'cumprofit']
                profit_data.reset_index().to_excel(
                    writer, sheet_name='layering-test-result', index=False)

    def turnover_test(self, canvas, grouper: pd.Series = None):
        if grouper is None:
            self.turnover.unstack().drawer.draw('line', ax=canvas, title='layering turnover')

    def test_scatter(self, canvas, scatter_period: 'int | str' = -1, grouper: pd.Series = None):
        data = pd.concat(
            [self.factor_data, self.forward_return, grouper], axis=1)
        cross_section_period = data.dropna().index.get_level_values(0).unique()
        if isinstance(scatter_period, int):
            scatter_data = pd.concat([self.factor_data, self.forward_return], axis=1).loc[
                (cross_section_period[scatter_period], slice(None)), :]
        elif isinstance(scatter_period, str):
            scatter_data = pd.concat([self.factor_data, self.forward_return], axis=1).loc[
                (scatter_period, slice(None)), :]
        scatter_data.drawer.draw('scatter', x=self.factor_data.name, y=self.forward_return.name,
                                 ax=canvas, datetime=scatter_data.index.get_level_values(0)[0], s=1)

    def __call__(self, grouper: pd.Series = None, benchmark: pd.Series = None):
        if grouper is not None:
            draw_number = 6
        else:
            draw_number = 4
        with(
            pq.Gallery(draw_number, 1, show=self.show, path=self.imagepath, grouper=grouper) as g,
            pd.ExcelWriter(self.datapath) as w
        ):
            self.barra_test(g.axes[0, 0], w, grouper=grouper)
            self.ic_test(g.axes[1, 0], w, grouper=grouper)
            self.layering_test(
                g.axes[2, 0], w, grouper=grouper, benchmark=benchmark)
            self.turnover_test(g.axes[3, 0], grouper=grouper)
            if grouper is not None:
                self.cross_sectional_test(g.axes[4, 0], grouper=grouper)
                self.test_scatter(g.axes[5, 0],  grouper=grouper)


if __name__ == "__main__":
    pass
    # from factor.define.valuation import Ep
    # open_price = pq.Stock.market_daily('20210101', '20210131', fields='open')
    # forward_return = open_price.groupby(
    #     level=1).shift(-1) / open_price.groupby(level=1).shift(-2) - 1
    # industry = pq.Stock.plate_info(
    #     '20210101', '20210131', fields='citi_industry_name1')
    # factor_data = get_factor_data(Ep(), pq.Stock.trade_date('20210101', '20210131',
    #     fields='trading_date').trading_date.tolist())
    # factor_analysis(factor_data.ep, forward_return.open,
    #     industry.citi_industry_name1, q=5, ic_grouped=True, commission=0)
    # price_1_priod = pq.Stock.market_daily('2023-01-04',
    #         '2023-01-04', fields='adj_open').droplevel(0)
    # print(price_1_priod)

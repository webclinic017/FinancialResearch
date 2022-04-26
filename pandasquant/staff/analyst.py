import numpy as np
import pandas as pd
import statsmodels.api as sm
from ..tools import *


@pd.api.extensions.register_dataframe_accessor("regressor")
@pd.api.extensions.register_series_accessor("regressor")
class Regressor(Worker):
    '''Regressor is a staff worker in pandasquant, used for a dataframe
    to perform regression analysis in multiple ways, like ols, logic,
    and so on.
    '''
    
    def ols(self, other: pd.Series = None):
        '''OLS Regression Function
        ---------------------------

        x_col: list, a list of column names used for regression x values
        y_col: str, the column name used for regression y values
        '''
        def _reg(data):
            y = data.iloc[:, -1]
            x = data.iloc[:, :-1]
            x = sm.add_constant(x)
            model = sm.OLS(y, x).fit()
            t = pd.Series(model.tvalues)
            coef = pd.Series(model.params)
            res = pd.concat([coef, t], axis=1)
            res.columns = ['coef', 't']
            return res
        
        if self.type_ == Worker.SERIES_PANEL and other is not None:
            reg_data = pd.merge(self.data, other, left_index=True, right_index=True)
            return reg_data.groupby(level=0).apply(_reg)
        elif self.type_ == Worker.SERIES_PANEL and other is None:
            raise ValueError('Target Series is required')
        elif self.type_ == Worker.FRAME_PANEL:
            return reg_data.groupby(level=0).apply(_reg)
        else:
            return _reg(self.data)

@pd.api.extensions.register_dataframe_accessor("describer")
@pd.api.extensions.register_series_accessor("describer")
class Describer(Worker):
    '''Describer is a staff worker in pandasquant, used for a dataframe
    or a series to perform a series of descriptive analysis, like
    correlation analysis, and so on.
    '''

    def corr(self, other: pd.Series = None, method: str = 'spearman', tvalue = False):
        '''Calculation for correlation matrix
        -------------------------------------

        method: str, the method for calculating correlation function
        tvalue: bool, whether to return t-value of a time-seriesed 
            correlation coefficient
        '''
        if self.type_ in [Worker.FRAME_CROSSSECTION, Worker.FRAME_TIMESERIES]:
            corr = self.data.corr(method=method)
        elif self.type_ in [Worker.SERIES_CROSSSECTION, Worker.SERIES_TIMESERIES]:
            corr = self.data.corr(other, method=method)
        elif self.type_ == Worker.FRAME_PANEL:
            corr = self.data.groupby(level=0).corr(method=method)
        elif self.type_ == Worker.SERIES_PANEL:
            corr = self.data.groupby(level=0).corr(other, method=method)

        if tvalue:
            n = corr.index.levels[0].size
            mean = corr.groupby(level=1).mean()
            std = corr.groupby(level=1).std()
            t = mean / std * np.sqrt(n)
            t = t.loc[t.columns, t.columns].replace(np.inf, np.nan).replace(-np.inf, np.nan)
            return t
            
        return corr

    def ic(self, other: pd.Series = None, method: str = 'spearman'):
        '''To calculate ic value
        ------------------------

        other: series, the forward column
        method: str, 'spearman' means rank ic
        '''
        if self.type_ == Worker.SERIES_CROSSSECTION:
            ic = self.data.corr(other, method=method)
        elif self.type_ == Worker.SERIES_PANEL:
            ic = self.data.groupby(level=0).corr(other, method=method)
        elif self.type_ == Worker.FRAME_CROSSSECTION:
            ic_data = pd.merge(self.data, other, left_index=True, right_index=True)
            ic = ic_data.corr(method=method)
        elif self.type_ == Worker.FRAME_PANEL:
            ic_data = pd.merge(self.data, other, left_index=True, right_index=True)
            ic = ic_data.groupby(level=0).corr()
            ic = ic_data.loc[(slice(None), ic.columns.difference(self.data.columns)), self.data.columns].droplevel(1)
        else:
            ic = self.data.groupby(level=0).corr().loc[(slice(None), ret_col), :]
            ic = ic.droplevel(1).drop(ret_col, axis=1)
            return ic


if __name__ == "__main__":
    data = pd.DataFrame(np.random.rand(500, 5), index=pd.MultiIndex.from_product(
        [pd.date_range('20100101', periods=100), list('abcde')]
    ), columns=['id1', 'id2', 'id3', 'id4', 'id5'])
    print(data.describer.corr(tvalue=True))

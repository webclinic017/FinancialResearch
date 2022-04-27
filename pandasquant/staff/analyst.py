import numpy as np
import pandas as pd
import statsmodels.api as sm
from ..tools import *


class AnalystError(FrameWorkError):
    pass

@pd.api.extensions.register_dataframe_accessor("regressor")
@pd.api.extensions.register_series_accessor("regressor")
class Regressor(Worker):
    '''Regressor is a staff worker in pandasquant, used for a dataframe
    to perform regression analysis in multiple ways, like ols, logic,
    and so on.
    '''
    
    def ols(self, other: pd.Series = None, x_col: str = None, y_col: str = None):
        '''OLS Regression Function
        ---------------------------

        other: Series, assigned y value in a series form
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

        param_status = (other is not None, x_col is not None, y_col is not None)

        if param_status == (True, True, True):
            raise AnalystError('ols', "You can assign either other or x_col and y_col, but not both.")
        elif param_status == (True, False, False):
            data = pd.merge(self.data, other, left_index=True, right_index=True)
        elif param_status == (False, True, True):
            data = self.data.loc[:, x_col + [y_col]]
        else:
            raise AnalystError('ols', "You need to assign x_col and y_col both.")

        if self.type_ == Worker.PN:
            return data.groupby(level=0).apply(_reg)
        else:
            return _reg(data)
            
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
        tvalue: bool, whether to return t-value of a time-seriesed correlation coefficient
        '''
        if other is not None:
            data = pd.merge(self.data, other, left_index=True, right_index=True)
        else:
            data = self.data

        if self.type_ == Worker.PN:
            corr = data.groupby(level=0).corr(method=method)
            if tvalue:
                n = corr.index.levels[0].size
                mean = corr.groupby(level=1).mean()
                std = corr.groupby(level=1).std()
                t = mean / std * np.sqrt(n)
                t = t.loc[t.columns, t.columns].replace(np.inf, np.nan).replace(-np.inf, np.nan)
                return t
            return corr
        else:
            return data.corr(method=method)

    def ic(self, other: pd.Series = None, method: str = 'spearman'):
        '''To calculate ic value
        ------------------------

        other: series, the forward column
        method: str, 'spearman' means rank ic
        '''
        if other is not None:
            data = pd.merge(self.data, other, left_index=True, right_index=True)
        else:
            data = self.data
        
        if self.type_ == Worker.PN:
            ic = data.groupby(level=0).corr(method=method)
            ic = ic.loc[(slice(None), ic.columns[-1]), ic.columns[:-1]].droplevel(1)
            return ic

        elif self.type_ == Worker.CS:
            ic = data.corr(method=method)
            ic = ic.loc[ic.columns[:-1], ic.columns[-1]]
            return ic
        
        else:
            raise AnalystError('ic', 'Timeseries data cannot be used to calculate ic value!')


if __name__ == "__main__":
    panelframe = pd.DataFrame(np.random.rand(500, 5), index=pd.MultiIndex.from_product(
        [pd.date_range('20100101', periods=100), list('abcde')]
    ), columns=['id1', 'id2', 'id3', 'id4', 'id5'])
    panelseries = pd.Series(np.random.rand(500), index=pd.MultiIndex.from_product(
        [pd.date_range('20100101', periods=100), list('abcde')]
    ), name='id1')
    tsframe = pd.DataFrame(np.random.rand(500, 5), index=pd.date_range('20100101', periods=500),
        columns=['id1', 'id2', 'id3', 'id4', 'id5'])
    tsseries = pd.Series(np.random.rand(500), index=pd.date_range('20100101', periods=500), name='id1')
    csframe = pd.DataFrame(np.random.rand(5, 5), index=list('abcde'), 
        columns=['id1', 'id2', 'id3', 'id4', 'id5'])
    csseries = pd.Series(np.random.rand(5), index=list('abcde'), name='id1')
    print(csseries.describer.corr(csseries))

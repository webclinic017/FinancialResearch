import numpy as np
import pandas as pd
import statsmodels.api as sm
from ..tools import *


@pd.api.extensions.register_dataframe_accessor("regressor")
class Regressor(Worker):
    '''Regressor is a staff worker in pandasquant, used for a dataframe
    to perform regression analysis in multiple ways, like ols, logic,
    and so on.
    '''
    
    def ols(self, x_col: 'list', y_col: 'str'):
        '''OLS Regression Function
        ---------------------------

        x_col: list, a list of column names used for regression x values
        y_col: str, the column name used for regression y values
        '''
        def _reg(d):
            x = d.loc[:, x_col]
            x = sm.add_constant(x)
            y = d.loc[:, y_col]
            model = sm.OLS(y, x).fit()
            t = pd.Series(model.tvalues)
            coef = pd.Series(model.params)
            return pd.concat([t, coef], axis=1)
        
        if self.type_ == Worker.PANEL:
            return self.dataframe.groupby(level=0).apply(_reg)
        else:
            return _reg(self.dataframe)

@pd.api.extensions.register_dataframe_accessor("describer")
@pd.api.extensions.register_series_accessor("describer")
class Describer(Worker):
    '''Describer is a staff worker in pandasquant, used for a dataframe
    or a series to perform a series of descriptive analysis, like
    correlation analysis, and so on.
    '''

    def corr(self, method: str = 'spearman', tvalue = False):
        '''Calculation for correlation matrix
        -------------------------------------

        method: str, the method for calculating correlation function
        tvalue: bool, whether to return t-value of a time-seriesed 
            correlation coefficient
        '''
        if self.type_ != Worker.PANEL:
            return self.data.corr(method=method)
        
        else:
            tcor = self.data.groupby(level=0).corr()
            
            if tvalue:
                n = tcor.index.levels[0].size
                mean = tcor.groupby(level=1).mean()
                std = tcor.groupby(level=1).std()
                t = mean / std * np.sqrt(n)
                t = t.loc[t.columns, t.columns].replace(np.inf, np.nan).replace(-np.inf, np.nan)
                return t
                
            return tcor

    def ic(self, method: str = 'spearman', ret_col: str = 'fwd'):
        '''To calculate ic value
        ------------------------

        method: str, 'spearman' means rank ic
        '''
        if self.type_ != Worker.PANEL:
            return self.data.corr(method=method)
        
        else:
            ic = self.data.groupby(level=0).corr().loc[(slice(None), ret_col), :]
            ic = ic.droplevel(1).drop(ret_col, axis=1)
            return ic


if __name__ == "__main__":
    pass

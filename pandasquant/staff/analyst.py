import numpy as np
import pandas as pd
import statsmodels.api as sm
from ..tools import *


@pd.api.extensions.register_dataframe_accessor("regressor")
class Regressor(Worker):

    def ols(self, x_col: 'list', y_col: 'str'):
        ''''''
        def _reg(d):
            x = d.loc[:, x_col]
            x = sm.add_constant(x)
            y = d.loc[:, y_col]
            model = sm.OLS(y, x).fit()
            t = pd.Series(model.tvalues)
            coef = pd.Series(model.params)
            return pd.concat([t, coef], axis=1)
        
        if self.type_ == Worker.TIMESERIES:
            return _reg(self.dataframe)
        
        elif self.type_ == Worker.PANEL:
            return self.dataframe.groupby(level=0).apply(_reg)
            
        else:
            # 截面数据也应当支持回归，这里截面回归也可以用_reg
            raise TypeError('ols currently only support for panel and timeseries data')

@pd.api.extensions.register_dataframe_accessor("describer")
class Describer(Worker):

    def corr(self, method: str = 'spearman', tvalue = False):
        if self.type_ != Worker.PANEL:
            return self.dataframe.corr(method=method)
        
        else:
            tcor = self.dataframe.groupby(level=0).corr()
            if tvalue:
                n = tcor.index.levels[0].size
                mean = tcor.groupby(level=1).mean()
                std = tcor.groupby(level=1).std()
                t = mean / std * np.sqrt(n)
                t = t.loc[t.columns, t.columns].replace(np.inf, np.nan).replace(-np.inf, np.nan)
                return t
            # 这里的else可以不要，注意代码风格
            else:
                return tcor


if __name__ == "__main__":
    pass

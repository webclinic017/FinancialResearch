import pandas as pd
from ..tools import *

@pd.api.extensions.register_dataframe_accessor("relocator")
@pd.api.extensions.register_series_accessor("relocator")
class Relocator(Worker):

    def profit(self, weight_col: str = None, forward_col: str = None):
        '''calculate profit from weight and forward
        ---------------------------------------------

        weight_col: str, the column name of weight
        forward_col: str, the column name of forward
        '''
        if self.type_ == Worker.TIMESERIES:
            raise TypeError('Please transform your data into multiindex data')
        
        elif self.type_ == Worker.CROSSSECTION:
            raise TypeError('We cannot calculate the profit by cross section data')
        
        elif self.type_ == Worker.PANEL and isinstance(self.data, pd.DataFrame):
            return self.data.groupby(level=0).apply(lambda x:
                (x.loc[:, weight_col] * x.loc[:, forward_col]).sum()
                / x.loc[:, weight_col].sum()
            )
        
        elif self.type_ == Worker.PANEL and isinstance(self.data, pd.Series):
            # if you pass a Series in a panel form, we assume that 
            # value is the forward return and weights are all equal
            return self.data.groupby(level=0).mean()

@pd.api.extensions.register_dataframe_accessor("backtester")
class Backtester(Worker):
    pass

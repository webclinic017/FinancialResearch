import pandas as pd
from ..tools import *

@pd.api.extensions.register_dataframe_accessor("relocator")
class Relocator(Worker):

    def profit(self, weight_col: str, forward_col: str):
        ''''''
        if self.type_ == Worker.TIMESERIES:
            raise TypeError('Please transform your data into multiindex data')
        
        elif self.type_ == Worker.CROSSSECTION:
            raise TypeError('We cannot calculate the profit by cross section data')
        
        else:
            return self.dataframe.groupby(level=0).apply(lambda x:
                (x.loc[:, weight_col] * x.loc[:, forward_col]).sum()
                / x.loc[:, weight_col].sum()
            )

@pd.api.extensions.register_dataframe_accessor("backtester")
class Backtester(Worker):
    pass

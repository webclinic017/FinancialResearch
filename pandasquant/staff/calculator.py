import pandas as pd
from ..tools import *


@pd.api.extensions.register_dataframe_accessor("calculator")
@pd.api.extensions.register_series_accessor("calculator")
class Calculator(Worker):
    
    def rolling(self, window: int, func, *args, grouper = None, **kwargs):
        '''Provide rolling window func apply for pandas dataframe
        ----------------------------------------------------------

        window: int, the rolling window length
        func: unit calculation function
        args: arguments apply to func
        grouper: the grouper applied in func
        kwargs: the keyword argument applied in func
        '''
        # in case of unsorted level
        data = self.data.sort_index().copy()

        if self.type_ == Worker.TS:
            datetime_index = data.index
        elif self.type_ == Worker.PN:
            datetime_index = data.index.levels[0]
        else:
            raise TypeError('rolling only support for panel or time series data')
        
        result = []
        for i in range(window - 1, datetime_index.size):
            window_data = data.loc[datetime_index[i - window + 1]:datetime_index[i]].copy()
            window_data.index = window_data.index.remove_unused_levels()
            if grouper is not None:
                window_result = window_data.groupby(grouper).apply(func, *args, **kwargs)
            else:
                window_result = func(window_data, *args, **kwargs)
            result.append(window_result)
        
        result = pd.concat(result)

        if grouper is not None:
            result = result.swaplevel(0, 1).sort_index()

        return result

from email.headerregistry import Group
import numpy as np
import pandas as pd
from ..tools import *


@pd.api.extensions.register_dataframe_accessor("preprocessor")
class PreProcessor(Worker):
    
    def price2ret(self, period: str, open_column: str = 'close', close_column: str = 'close'):
        if self.type_ == Worker.PANEL:
            # https://pandas.pydata.org/docs/reference/api/pandas.Grouper.html
            # https://stackoverflow.com/questions/15799162/
            close_price = self.dataframe.groupby([
                pd.Grouper(level=0, freq=period, label='right'),
                pd.Grouper(level=1)
            ]).last().loc[:, close_column]
            open_price = self.dataframe.groupby([
                pd.Grouper(level=0, freq=period, label='right'),
                pd.Grouper(level=1)
            ]).first().loc[:, open_column]

        elif self.type_ == Worker.TIMESERIES:
            close_price = self.dataframe.\
                resample(period, label='right').last()
            open_price = self.dataframe.\
                resample(period, label='right').first()

        return (close_price - open_price) / open_price

    def price2fwd(self, period: str, open_column: str = 'open', close_column: str = 'close'):
        if self.type_ == Worker.PANEL:
            # https://pandas.pydata.org/docs/reference/api/pandas.Grouper.html
            # https://stackoverflow.com/questions/15799162/
            close_price = self.dataframe.groupby([
                pd.Grouper(level=0, freq=period, label='left'),
                pd.Grouper(level=1)
            ]).last().loc[:, close_column]
            open_price = self.dataframe.groupby([
                pd.Grouper(level=0, freq=period, label='left'),
                pd.Grouper(level=1)
            ]).first().loc[:, open_column]

        elif self.type_ == Worker.TIMESERIES:
            close_price = self.dataframe.\
                resample(period, label='left').last()
            open_price = self.dataframe.\
                resample(period, label='left').first()

        return (close_price - open_price) / open_price
        
    def cum2diff(self, grouper = None, period: int = 1, axis: int = 0, keep: bool = True):
        def _diff(data):
            diff = data.diff(period, axis=axis)
            if keep:
                diff.iloc[:period] = data.iloc[:period]
            return diff
        
        if grouper is None:
            diff = _diff(self.dataframe)
        else:
            diff = self.dataframe.groupby(grouper).apply(lambda x: x.groupby(level=1).apply(_diff))
            
        return diff

    def dummy2category(self, dummy_columns, name: str = 'group'):
        columns = pd.DataFrame(
            dummy_columns.values.reshape((1, -1))\
            .repeat(self.dataframe.shape[0], axis=0),
            index=self.dataframe.index, columns=self.dataframe.columns
        )
        category = columns[self.dataframe.loc[:, dummy_columns].astype('bool')]\
            .replace(np.nan, '').astype('str').sum(axis=1)
        category.name = name
        return category

    def logret2algret(self):
        return np.exp(self.dataframe) - 1
    
    def algret2logret(self):
        return np.log(self.dataframe)

    def resample(self, rule: str, **kwargs):
        if self.type_ == Worker.TIMESERIES:
            return self.dataframe.resample(rule, **kwargs)
        elif self.type_ == Worker.PANEL:
            return self.dataframe.groupby([pd.Grouper(level=0, freq=rule, **kwargs), pd.Grouper(level=1)])

if __name__ == "__main__":
    import numpy as np
    price = pd.DataFrame(np.random.rand(500, 4), columns=['open', 'high', 'low', 'close'],
        index=pd.MultiIndex.from_product([pd.date_range('20100101', periods=100), list('abced')]))
    
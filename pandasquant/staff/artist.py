import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from ..tools import *


class Gallery():
    def __init__(self, nrows: int, ncols: int, figsize: tuple = None) -> None:
        self.nrows = nrows
        self.ncols = ncols
        self.figsize = (12 * ncols, 8 * nrows)

    def __enter__(self):
        fig, axes = plt.subplots(self.nrows, self.ncols, figsize=self.figsize)
        axes = np.array(axes).reshape((self.nrows, self.ncols))
        self.fig = fig
        self.axes = axes
        return (fig, axes)
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        for ax in self.axes.reshape(-1):
            ax.xaxis.set_major_locator(mticker.MaxNLocator())
        return False

@pd.api.extensions.register_dataframe_accessor("drawer")
class Drawer(Worker):

    def draw(self, kind: str, 
        datetime: str = slice(None), 
        asset: str = slice(None), 
        indicator: str = slice(None), 
        **kwargs):
        plotwised = self._indexer(datetime, asset, indicator)
        if not isinstance(plotwised, (pd.Series, pd.DataFrame)):
            raise ValueError('Your slice data seems not to be a plotable data')
        
        elif not isinstance(datetime, str):
            plotwised.index = plotwised.index.strftime(r'%Y-%m-%d')

        plotwised.plot(kind=kind, **kwargs)


if __name__ == "__main__":
    data = pd.DataFrame(np.random.rand(100,2), index=pd.date_range('20200101', periods=100), columns=list('ab'))
    with Gallery(1, 2) as (_, axes):
        data.drawer.draw('line', indicator='a', color='red', ax=axes[0,0])
        data.drawer.draw('bar', indicator='a', color='green', ax=axes[0, 0].twinx())
        data.drawer.draw('line', indicator='b', color='blue', ax=axes[0, 1])
    
    plt.savefig('test.png')
    
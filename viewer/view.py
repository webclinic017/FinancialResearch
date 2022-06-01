import json
import sys
import datetime
import pandasquant as pq
import mplfinance as mpf
import matplotlib.pyplot as plt
from .indicator import *
from .data import *


class Canvas:
    def __init__(self, code: str = None, fetcher: str = "stock",
                 config: 'dict | str' = None,
                 start: str = None, end: str = None,
                 subplot_names: 'list | str' = None,
                 subplot_kwargs: 'list | dict' = None,
                 plot_type: str = None,
                 show: bool = True,
                 figsize: tuple = None,
                 path: str = None) -> None:
        self.style = mpf.make_mpf_style(
                marketcolors=mpf.make_marketcolors(up="r", down="g",inherit=True),
                gridcolor="gray", gridstyle="--", gridaxis="both")  
        if config is None and code is not None:
            self.code = code
            self.fetcher = fetcher or "stock"
            self.end = start or datetime.datetime.today()
            self.start = end or self.end - datetime.timedelta(days=365)
            self.end = pq.time2str(self.end)
            self.start = pq.time2str(self.start)
            self.subplot_names = subplot_names or []
            self.subplot_kwargs = subplot_kwargs or []
            self.plot_type = plot_type or ("candle" if self.fetcher == "stock" else "line")
            self.show = show
            self.figsize = tuple(figsize) or (7, 5)
            self.path = path or False
        elif config is not None and code is None:
            if isinstance(config, str):
                config = json.load(config)
            self.code = config["code"]
            self.fetcher = config.get("fetcher", "stock")
            self.end = config.get("end", datetime.datetime.today())
            self.start = config.get("start", self.end - datetime.timedelta(days=365))
            self.end = pq.time2str(self.end)
            self.start = pq.time2str(self.start)
            self.subplot_names = config.get("subplot_names", [])
            self.subplot_kwargs = config.get("subplot_kwargs", [])
            self.plot_type = config.get("plot_type", ("candle" if self.fetcher == "stock" else "line"))
            self.show = config.get("show", True)
            self.figsize = tuple(config.get("figsize", (7, 5)))
            self.path = config.get("path", False)
        else:
            raise ValueError("config or code should be provided")
    
    def get_data(self):
        data_handler = eval(self.fetcher)
        self.data = data_handler(self.code, self.start, self.end)
    
    def calc_subplots(self):
        subplot_names = self.subplot_names
        subplot_data = []
        for img_num in range(len(subplot_names)):
            subplot_data.append([])
            for subplot_num in range(len(subplot_names[img_num])):
                subplot_func, subplot_params = subplot_names[img_num][subplot_num].split("_")[0],\
                    subplot_names[img_num][subplot_num].split("_")[1:]
                subplot_handler = eval(subplot_func)
                subplot_data[img_num].append(subplot_handler(self.data, *subplot_params))
        self.subplot_data = subplot_data

    def feed(self):
        addplot = []
        for img_num in range(len(self.subplot_names)):
            for subplot_num in range(len(self.subplot_names[img_num])):
                addplot.append(mpf.make_addplot(
                    self.subplot_data[img_num][subplot_num], 
                    **self.subplot_kwargs[img_num][subplot_num],
                    ))
        self.addplot = addplot
    
    def plot(self):
        fig, axes = mpf.plot(self.data, addplot=self.addplot, figsize=self.figsize, title=self.code, 
            returnfig=True, style=self.style, type=self.plot_type, volume=True, tight_layout=True)

        for i, ax in enumerate(axes[::2]):
            if i == 0:
                extra_line = 1
            else:
                extra_line = 0
            ax.legend([None] * (len(self.subplot_names[i]) + extra_line))
            handles = ax.get_legend().legendHandles
            ax.legend(handles=handles[extra_line:], labels=self.subplot_names[i])

        if self.path:
            plt.savefig(self.path)
        if self.show:
            plt.show()
        
    def __call__(self):
        self.get_data()
        self.calc_subplots()
        self.feed()
        self.plot()

if __name__ == "__main__":
    with open('viewer/config.json', 'r') as f:
        config = json.load(f)
    asset = sys.argv[1]
    Canvas(config=config[asset])()

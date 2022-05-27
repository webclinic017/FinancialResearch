import datetime
import argparse
import mplfinance as mpl
import pandasquant as pq
import pandas as pd
from matplotlib import pyplot as plt


style = mpl.make_mpf_style(marketcolors=mpl.make_marketcolors(up="r", down="g",inherit=True),
    gridcolor="gray", gridstyle="--", gridaxis="both")  

def parse_args():
    parser = argparse.ArgumentParser(description='Plot stock data.')
    parser.add_argument('-c', '--code', type=str, 
        default='600519.SH', help='The code of the stock, default to 600519.SH')
    parser.add_argument('-s', '--start', type=str, 
        default=(datetime.datetime.today() - datetime.timedelta(days=365)).strftime(r'%Y-%m-%d'),
        help='Start date, format: YYYY-MM-DD, default to one year ago')
    parser.add_argument('-e', '--end', type=str, 
        default=datetime.datetime.today().strftime(r'%Y-%m-%d'),
        help='End date, format: YYYY-MM-DD, default to today')
    parser.add_argument('-p', '--path', type=str, default='result.nosync/image/today.png',
        help='Path to save the plot, default: result.nosync/image/today.png')
    parser.add_argument('--figsize', type=str, default='24,8.4',
        help='Figure size, format: W,H, default: 24,8.4')
    parser.add_argument('--show', type=bool, default=True,
        help='Whether to show the plot, default: True')
    args = parser.parse_args()
    return args

def get_data(code: str, start: str, end: str):
    return pq.Api.market_daily(start=start, end=end, 
        fields=['n_open', 'n_high', 'n_low', 'n_close', 'n_volume'], 
        conditions='c_windCode=%s' % code).rename(columns=
        lambda x: pq.hump2snake(x.split('.')[-1])).droplevel(1) 

def calculate(data: pd.DataFrame):
    ma5 = data['close'].rolling(5).mean()
    ma10 = data['close'].rolling(10).mean()
    bollmid = data['close'].rolling(20).mean()
    dev = data['close'].rolling(20).std()
    bolltop = bollmid + 2 * dev
    bollbot = bollmid - 2 * dev
    atr = (data['high'] / data['low'] - 1).rolling(14).mean()
    return {
        "MA5": ma5,
        "MA10": ma10,
        "BBMid": bollmid,
        "BBTop": bolltop,
        "BBBot": bollbot,
        "ATR": atr,
        }

def addplots(indicators: dict):
    return [
        {
            "MA5": mpl.make_addplot(indicators['MA5'], color='orange'),
            "MA10": mpl.make_addplot(indicators['MA10'], color='blue'),
            "BBMid": mpl.make_addplot(indicators['BBMid'], color='purple'),
            "BBTop": mpl.make_addplot(indicators['BBTop'], linestyle='--', color='red'),
            "BBBot": mpl.make_addplot(indicators['BBBot'], linestyle='--', color='green'),
        }, 
        {},
        {
            "ATR": mpl.make_addplot(indicators['ATR'], panel=2),
        },]

def config(addplot: list[dict]):
    aplt = [ap.values() if ap else [] for ap in addplot]
    addplots = []
    for ap in aplt:
        addplots += ap
    return addplots

def plot(code: str, data: pd.DataFrame, addplot: list[dict], addplot_: list,
    figsize: list[float], path: str, show: bool):
    fig, axes = mpl.plot(data, addplot=addplot_, figsize=figsize, title=code, 
        returnfig=True, style=style, type='candle', volume=True)

    for i, ax in enumerate(axes[::2]):
        if i == 0:
            extra_line = 2
        else:
            extra_line = 0
        ax.legend([None] * (len(addplot[i]) + extra_line))
        handles = ax.get_legend().legendHandles
        ax.legend(handles=handles[extra_line:], labels=list(addplot[i].keys()))

    plt.savefig(path)
    if show:
        plt.show()

if __name__ == "__main__":
    args = parse_args()
    code = args.code
    start = args.start
    end = args.end
    path = args.path
    figsize = tuple(map(float, args.figsize.split(',')))
    show = args.show

    data = get_data(code, start, end)
    indicators = calculate(data)
    addplot = addplots(indicators)
    addplot_ = config(addplot)
    plot(code, data, addplot, addplot_, figsize, path, show)

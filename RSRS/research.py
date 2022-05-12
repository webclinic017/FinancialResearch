import pandasquant as pq
import pandas as pd
import matplotlib.pyplot as plt


def calc_unit(data):
    print(f'[*] Calculating {data.index.get_level_values(0)[-1]}')
    result = data.groupby(level=1).apply(lambda x: x.droplevel(1).regressor.ols(x_col='low', y_col='high'))
    return result.loc[(slice(None), "low"), "coef"].droplevel(1)
    
data = pq.Stock.market_daily(start='20180101', end='20211231',
    fields=['open', 'high', 'low', 'close'])
factor = data.calculator.rolling(window=21, func=calc_unit, interval=21)
# factor.to_frame().to_parquet('RSRS/data/facotr.parquet')
forward = data.converter.price2fwd(period=21, open_col='open', close_col='close')
# forward = forward.to_frame(name='forward').to_parquet('RSRS/data/forward.parquet')
# factor = pd.read_parquet('RSRS/data.nosync/factor.parquet')
# forward = pd.read_parquet('RSRS/data.nosync/forward.parquet')
ic = factor.describer.ic(forward=forward)
quantiles = factor.groupby(level=0).apply(lambda x: pd.qcut(x, q=5, labels=False) + 1)
profit = forward.relocator.profit(grouper=quantiles)
with pq.Gallery(1, 1) as g:
    ax = g.axes[0, 0]
    profit.drawer.draw('bar', ax=ax)
    (profit + 1).drawer.draw('line', ax=ax.twinx())
# ic.drawer.draw('bar')

import pandas as pd
import backtrader as bt
import pandasquant as pq
from .datas import *
from .indicators import *
from .strategies import *


# Get dataset
data = etffeedsina('sh510300')

# Create a cerebro entity
cerebro = bt.Cerebro()

# Cheat on close
cerebro.broker.set_coc(True)

# Add money
cerebro.broker.setcash(1000000)

# Feed data to the cerebro
cerebro.adddata(data)

# Add indicators
# cerebro.addindicator(Grid, period=20)

# Add strategy
cerebro.addstrategy(GridStrategy)
# cerebro.addstrategy(SMACrossStrategy)

# Add observers
cerebro.addobserver(bt.observers.DrawDown)
cerebro.addobserver(bt.observers.Benchmark)

# Add analyzers
cerebro.addanalyzer(bt.analyzers.SharpeRatio)
cerebro.addanalyzer(bt.analyzers.TimeDrawDown)
cerebro.addanalyzer(bt.analyzers.TimeReturn)
cerebro.addanalyzer(pq.OrderTable)

# Run the strategy
result = cerebro.run()

# Show analyze results
timereturn = pd.Series(result[0].analyzers.timereturn.rets)
print(result[0].analyzers.sharperatio.rets)
print(result[0].analyzers.timedrawdown.rets)
print(timereturn)
print(result[0].analyzers.ordertable.rets)

# Visualize the total strategy result
cerebro.plot(style='candle')

# Visualize the networth curve
with pq.Gallery(1, 1, figsize=(12, 8)) as g:
    ax = g.axes[0, 0]
    (timereturn + 1).cumprod().drawer.draw(kind='line', ax=ax)
    ax.hlines(xmin=ax.get_xlim()[0], xmax=ax.get_xlim()[1], y=1, color='grey', linestyle='--')
    timereturn.drawer.draw(kind='bar', ax=ax.twinx())

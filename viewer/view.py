import pandas as pd
import backtrader as bt
import pandasquant as pq
import matplotlib.pyplot as plt
from .datas import *
from .indicators import *
from .strategies import *


# Get dataset
data = etffeedsina('sh510300', '2019-01-01')

# Create a cerebro entity
cerebro = bt.Cerebro(stdstats=False)

# Cheat on close
# cerebro.broker.set_coc(True)

# Add money
cerebro.broker.setcash(1000000)

# Feed data to the cerebro
cerebro.adddata(data)

# Add indicators
# cerebro.addindicator(SMACombination)
# cerebro.addindicator(ATRinc)
# cerebro.addindicator(KShape)

# Add strategy
# cerebro.addstrategy(GridStrategy, cashnum=5)
# cerebro.addstrategy(SMACrossStrategy)
cerebro.addstrategy(TurtleStrategy)

# Add observers
cerebro.addobserver(bt.observers.Broker)
# cerebro.addobserver(bt.observers.Trades)
cerebro.addobserver(bt.observers.BuySell)
cerebro.addobserver(bt.observers.DrawDown)
# cerebro.addobserver(bt.observers.Benchmark)

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
# _, ax = plt.subplots(1, 1, figsize=(12, 8))
# ax.plot((timereturn + 1).cumprod())
# ax.hlines(xmin=ax.get_xlim()[0], xmax=ax.get_xlim()[1], y=1, color='grey', linestyle='--')
# ax.twinx().bar(timereturn.index, timereturn.values)
# plt.show()

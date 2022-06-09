import backtrader as bt
import pandasquant as pq


if __name__ == '__main__':
    # Get dataset
    data = pq.Feed.from_api('600392.SH', '20170101', '20220601')
    # Create a cerebro entity
    cerebro = bt.Cerebro(stdstats=False)
    # Feed data to the cerebro
    cerebro.adddata(data)
    # Add indicators
    cerebro.addindicator(bt.indicators.SMA, period=5)
    cerebro.addindicator(bt.indicators.SMA, period=10)
    cerebro.addindicator(bt.indicators.SMA, period=20)
    cerebro.addindicator(bt.indicators.SMA, period=250)
    cerebro.addindicator(bt.indicators.ATR, period=10)
    # Visualization
    cerebro.run()
    cerebro.plot(style='candle')
    
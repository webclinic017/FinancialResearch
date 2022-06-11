import backtrader as bt


class Grid(bt.Indicator):
    lines = ('grid', 'level1', 'level2', 'level3', 'level4', 'level5')
    params = (('period', 10),)
    plotinfo = dict(subplot=False)
    plotlines = dict(grid=dict(_method='bar'))

    def __init__(self):
        self.high = bt.indicators.Highest(self.data.high, period=self.p.period)
        self.low = bt.indicators.Lowest(self.data.low, period=self.p.period)
        self.lines.gridlevel = [(self.high - self.low) * i / 5 + self.low for i in range(1, 5)]
        self.lines.level1 = self.lines.gridlevel[0]
        self.lines.level2 = self.lines.gridlevel[1]
        self.lines.level3 = self.lines.gridlevel[2]
        self.lines.level4 = self.lines.gridlevel[3]
        self.lines.grid = bt.Sum(*[self.data.close > self.gridlevel[i] for i in range(len(self.gridlevel))])


class SMACombination(bt.Indicator):
    lines = ('sma5', 'sma10', 'sma20', 'sma60', 'sma120', 'sma250')
    plotinfo = dict(subplot=True)

    def __init__(self) -> None:
        self.lines.sma5 = bt.indicators.SMA(period=5)
        self.lines.sma10 = bt.indicators.SMA(period=10)
        self.lines.sma20 = bt.indicators.SMA(period=20)
        self.lines.sma60 = bt.indicators.SMA(period=60)
        self.lines.sma120 = bt.indicators.SMA(period=120)
        self.lines.sma250 = bt.indicators.SMA(period=250)


class KShape(bt.Indicator):
    lines = ('hammer', 'hangingman', 'piercing', )
    plotinfo = dict(subplot=True)

    def __init__(self):
        self.lines.hammer = bt.talib.CDLHAMMER(self.data.open, 
            self.data.high, self.data.low, self.data.close)
        self.lines.hangingman = bt.talib.CDLHANGINGMAN(self.data.open,
            self.data.high, self.data.low, self.data.close)
        self.lines.piercing = bt.talib.CDLPIERCING(self.data.open,
            self.data.high, self.data.low, self.data.close)

class ATRinc(bt.Indicator):
    lines = ('atrinc5', 'atrinc10')

    def __init__(self) -> None:
        atr = bt.indicators.ATR(period=14) 
        atrinc = (atr - atr(-1)) / atr(-1)
        self.lines.atrinc5 = bt.indicators.SMA(atrinc, period=10)
        self.lines.atrinc10 = bt.indicators.SMA(atrinc, period=60)

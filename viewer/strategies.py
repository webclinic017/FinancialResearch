import numpy as np
import backtrader as bt
import pandasquant as pq
from .indicators import *


class SMACrossStrategy(pq.Strategy):
    def __init__(self):
        sma5 = bt.indicators.SMA(period=5)
        sma10 = bt.indicators.SMA(period=10)
        self.buycross = bt.And(sma5(-1) <= sma10(-1), sma5 > sma10)
        self.sellcross = bt.And(sma5(-1) >= sma10(-1), sma5 < sma10)
    
    def next(self):
        if self.buycross[0]:
            self.order_target_percent(target=1)

        elif self.sellcross[0]:
            self.order_target_percent(target=0)


class GridStrategy(pq.Strategy):
    params = (('cashnum', 5),)
    
    def __init__(self) -> None:
        self.grids = Grid(period=20)
        self.levels = [self.grids.level1, self.grids.level2, self.grids.level3, self.grids.level4, self.grids.level5]
        self.grid = self.grids.grid
        self.pregrid = self.grids.grid(-1)
        self.griddiff = self.grid - self.pregrid
        self.cashes = [self.broker.getcash() / self.p.cashnum for _ in range(self.p.cashnum)]
        self.holds = []
    
    def notify_order(self, order: bt.Order):
        if order.status in [order.Created, order.Accepted, order.Submitted]:
            return
        elif order.status in [order.Completed]:
            self.log(f'Trade <{order.executed.size}> at <{order.executed.price}>')
            if order.isbuy():
                self.cashes.pop()
                self.holds.append(order.executed.size)
            else:
                self.holds.pop()
                self.cashes.append(-order.executed.price * order.executed.size)
        elif order.status in [order.Canceled, order.Margin, order.Rejected, order.Expired]:
            self.log(f'order failed to execute')

    def buygrid(self, grid: int):
        if self.cashes:
            if grid == 0:
                self.order = self.buy(size=self.cashes[-1] // self.data.low[0],
                    exectype=bt.Order.Limit, price=self.data.low[0])
            else:
                self.order = self.buy(size=self.cashes[-1] // self.levels[int(grid - 1)][0],
                    exectype=bt.Order.Limit, price=self.levels[int(grid - 1)][0])
        else:
            self.log(f'Grid drop, no cash to buy', hint='WARN')

    def sellgrid(self, grid: int):
        if self.holds:
            if grid == 4:
                self.order = self.sell(size=self.holds[-1], exectype=bt.Order.Limit, price=self.data.high[0])
            else:
                self.order = self.sell(size=self.holds[-1], exectype=bt.Order.Limit, price=self.levels[int(grid)][0])
        else:
            self.log(f'Grid raise, no holds to sell', hint='WARN')

    def nextstart(self):
        self.log(f'start with {self.grid[0]}')
        self.buygrid(self.grid[0])
    
    def next(self):
        if self.griddiff[0] < 0:
            if self.order.status not in [self.order.Canceled, self.order.Completed, self.order.Rejected, self.order.Expired]:
                self.cancel(self.order)
            self.buygrid(self.grid[0])
        elif self.griddiff[0] > 0:
            if self.order.status not in [self.order.Canceled, self.order.Completed, self.order.Rejected, self.order.Expired]:
                self.cancel(self.order)
            self.sellgrid(self.grid[0])

class TurtleStrategy(bt.Strategy):
    
    def __init__(self) -> None:
        self.atr = bt.indicators.ATR(period=14)
        self.unit = 0.1
        self.currentpos = 0
        self.order = None
        self.lastbuyprice = np.inf
        self.alreadybuy = False
        high = bt.indicators.Highest(self.data.high, period=20)
        self.buysig = high(-1) <= self.data.close

    def next(self):
        if self.buysig[0] and not self.alreadybuy:
            self.order_target_percent(target=self.currentpos + self.unit)

        elif self.alreadybuy and self.data.close[0] >= self.lastbuyprice + self.atr[0]:
            self.order_target_percent(target=self.currentpos + self.unit)

    def notify_order(self, order):
        if order.status in [order.Created, order.Accepted, order.Submitted]:
            return
        elif order.status in [order.Completed]:
            self.log(f'Trade <{order.executed.size}> at <{order.executed.price}>')
            if order.isbuy():
                self.currentpos += self.unit
                self.lastbuyprice = order.executed.price
                self.alreadybuy = True
                if len(self.broker.pending) > 0:
                    # price = self.broker.pending[0].price + self.atr[0]
                    self.cancel(self.broker.pending[0])
                # else:
                price = order.executed.price - self.atr[0]
                self.sell(size=self.getposition().size, 
                    exectype=bt.Order.Stop, price=price)
            else:
                self.alreadybuy = False
                self.currentpos = 0

    def log(self, msg: str, hint: str = 'INFO'):
        dt = self.data.datetime.date(0)
        print(f'[{hint}] {dt} {msg}')

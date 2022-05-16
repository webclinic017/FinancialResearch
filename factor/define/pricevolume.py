import numpy as np
import pandas as pd
import pandasquant as pq
from factor.define.base import FactorBase


class HAphla(FactorBase):
    def __init__(self, period):
        name = 'haplha_' + str(period)
        self.period = period
        super().__init__(name)
    
    def calculate(self, date):
        last_date = pq.Stock.nearby_n_trade_date(date, -self.period + 1)
        index_price = pq.Stock.index_market_daily(last_date, date,
            fields='pct_change', code='000001.SH').droplevel(1)['pct_change']
        stock_price = pq.Stock.market_daily(last_date, date,
            fields='pct_change')['pct_change']
        self.factor = stock_price.groupby(level=1).apply(lambda x:
            x.droplevel(1).regressor.ols(index_price).loc["const", "coef"]
            if len(x) >= 30 else np.nan)

class HBeta(FactorBase):
    def __init__(self, period):
        name = 'hbeta_' + str(period)
        self.period = period
        super().__init__(name)
    
    def calculate(self, date):
        last_date = pq.Stock.nearby_n_trade_date(date, -self.period + 1)
        index_price = pq.Stock.index_market_daily(last_date, date,
            fields='pct_change', code='000001.SH').droplevel(1)['pct_change']
        stock_price = pq.Stock.market_daily(last_date, date,
            fields='pct_change')['pct_change']
        self.factor = stock_price.groupby(level=1).apply(lambda x:
            x.droplevel(1).regressor.ols(index_price).iloc[-1, 0]
            if len(x) >= 30 else np.nan)

class Momentum(FactorBase):
    def __init__(self, period: int):
        name = 'momentum_' + str(period)
        self.period = period
        super().__init__(name)
    
    def calculate(self, date):
        last_date = pq.Stock.nearby_n_trade_date(date, -self.period + 1)
        price_lastdate = pq.Stock.market_daily(last_date, last_date,
            fields='adj_close').droplevel(0).adj_close
        price_date = pq.Stock.market_daily(date, date,
            fields='adj_close').droplevel(0).adj_close
        self.factor = (price_date - price_lastdate) / price_lastdate

class WeightedMomentum(FactorBase):
    def __init__(self, period: int):
        name = 'weightedmomentum_' + str(period)
        self.period = period 
        super().__init__(name)
    
    def calculate(self, date):
        last_date = pq.Stock.nearby_n_trade_date(date, -self.period + 1)
        change = pq.Stock.market_daily(last_date, date,
            fields='pct_change')['pct_change']
        turnover = pq.Stock.derivative_indicator(last_date, date,
            fields='s_dq_turn').s_dq_turn
        self.factor = pd.concat([change, turnover], axis=1)\
            .groupby(level=1).apply(lambda x: 
                (x['pct_change'] * x['s_dq_turn']).mean())

class ExpWeightedMomentum(FactorBase):
    def __init__(self, period: int):
        name = 'expweightedmomentum_' + str(period)
        self.period = period
        super().__init__(name)
    
    def calculate(self, date):
        last_date = pq.Stock.nearby_n_trade_date(date, -self.period + 1)
        change = pq.Stock.market_daily(last_date, date,
            fields='pct_change')['pct_change']
        turnover = pq.Stock.derivative_indicator(last_date, date,
            fields='s_dq_turn').s_dq_turn
        exp = np.exp(np.arange(-self.period + 1, 1) / self.period / 4)
        self.factor = pd.concat([change, turnover], axis=1)\
            .groupby(level=1).apply(lambda x: 
                (x['pct_change'] * x['s_dq_turn'] * exp).sum()
                if len(x) == self.period else np.nan)

class LogPrice(FactorBase):
    def __init__(self):
        super().__init__('logprice')
    
    def calculate(self, date):
        price = pq.Stock.market_daily(date, date, 
            fields='close').droplevel(0).close
        self.factor = np.log(price)


if __name__ == "__main__":
    import time
    factor = LogPrice()
    start = time.time()
    print(factor('20200106'))
    print(f'time: {time.time() - start:.2f}s')

import numpy as np
import pandas as pd
import pandasquant as pq
from factor.define.base import FactorBase


class Momentum(FactorBase):
    def __init__(self, period: int):
        name = 'momentum_' + str(period)
        self.period = period
        super().__init__(name)
    
    def calculate(self, date):
        last_date = pq.Stock.nearby_n_trade_date(date, -self.period)
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
        last_date = pq.Stock.nearby_n_trade_date(date, -self.period)
        change = pq.Stock.market_daily(last_date, date,
            fields='pct_change')['pct_change']
        turnover = pq.Stock.derivative_indicator(last_date, date,
            fields='s_dq_turn').s_dq_turn
        self.factor = pd.concat([change, turnover], axis=1)\
            .groupby(level=1).apply(lambda x: 
                (x['pct_change'] * x['s_dq_turn']).mean()
            )

class ExpWeightedMomentum(FactorBase):
    def __init__(self, period: int):
        name = 'expweightedmomentum_' + str(period)
        self.period = period - 1
        super().__init__(name)
    
    def calculate(self, date):
        last_date = pq.Stock.nearby_n_trade_date(date, -self.period)
        change = pq.Stock.market_daily(last_date, date,
            fields='pct_change')['pct_change']
        turnover = pq.Stock.derivative_indicator(last_date, date,
            fields='s_dq_turn').s_dq_turn
        exp = np.exp(np.arange(-self.period + 1, 1) / self.period / 4)
        self.factor = pd.concat([change, turnover], axis=1)\
            .groupby(level=1).apply(lambda x: 
                (x['pct_change'] * x['s_dq_turn'] * exp).sum()
            )


if __name__ == "__main__":
    factor = ExpWeightedMomentum(5)
    print(factor('20200106'))

import pandasquant as pq
from factor.define.base import FactorBase


class Std(FactorBase):
    def __init__(self, period: int):
        name = 'std_' + str(period)
        self.period = period
        super().__init__(name)
    
    def calculate(self, date):
        last_date = pq.Stock.nearby_n_trade_date(date, -self.period)
        change = pq.Stock.market_daily(last_date, date,
            fields='pct_change')['pct_change']
        self.factor = change.groupby(level=1).std()

class FF3F(FactorBase):
    def __init__(self, period):
        name = 'ff3f_' + str(period)
        self.period = period
        super().__init__(name)

    def calculate(self, date):
        pass


if __name__ == "__main__":
    factor = Std(20)
    print(factor('20200106'))
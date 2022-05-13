import pandasquant as pq
from Factor.define.base import FactorBase


class Capital(FactorBase):
    def __init__(self):
        super().__init__('capital')
    
    def calculate(self, date):
        self.factor = pq.Stock.derivative_indicator(date, date,
            fields='s_val_mv').droplevel(0).s_val_mv

if __name__ == '__main__':
    factor = Capital()
    print(factor('20200106'))
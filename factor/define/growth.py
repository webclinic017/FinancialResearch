import pandasquant as pq
from Factor.define.base import FactorBase


class SalesGQ(FactorBase):
    def __init__(self):
        super().__init__('salesgq')
    
    def calculate(self, date):
        report_date = pq.nearest_report_period(date, 1)[0]
        self.factor = pq.Stock.financial_indicator(report_date, report_date, 
            fields='fa_yoy_or').droplevel(0).fa_yoy_or

class ProfitGQ(FactorBase):
    def __init__(self):
        super().__init__('profitgq')
    
    def calculate(self, date):
        report_date = pq.nearest_report_period(date, 1)[0]
        self.factor = pq.Stock.financial_indicator(report_date, report_date,
            fields='qfa_yoyprofit').droplevel(0).qfa_yoyprofit

class OcfGQ(FactorBase):
    def __init__(self):
        super().__init__('roegq')
    
    def calculate(self, date):
        report_dates = pq.nearest_report_period(date, 5)
        this_year = report_dates[-1]
        last_year = report_dates[0]
        ocf_thisyear = pq.Stock.cashflow_sheet(this_year, this_year,
            fields='net_cash_flows_oper_act').droplevel(0).net_cash_flows_oper_act
        ocf_lastyear = pq.Stock.cashflow_sheet(last_year, last_year,
            fields='net_cash_flows_oper_act').droplevel(0).net_cash_flows_oper_act
        self.factor = (ocf_thisyear - ocf_lastyear) / ocf_lastyear

class RoeGQ(FactorBase):
    def __init__(self):
        super().__init__('ocfgq')
    
    def calculate(self, date):
        report_date = pq.nearest_report_period(date, 1)[0]
        self.factor = pq.Stock.financial_indicator(report_date, report_date,
            fields='fa_yoyroe').droplevel(0).fa_yoyroe


if __name__ == '__main__':
    factor = RoeGQ()
    value = factor('2020-01-06')
    print(value)

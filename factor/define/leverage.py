import pandasquant as pq
from factor.define.base import FactorBase

class FinancialLeverage(FactorBase):
    def __init__(self):
        super().__init__('financialleverage')
    
    def calculate(self, date):
        report_date = pq.nearest_report_period(date)[0]
        asset = pq.Stock.balance_sheet(report_date, report_date, 
            fields='tot_assets').droplevel(0).tot_assets
        liab = pq.Stock.balance_sheet(report_date, report_date,
            fields='tot_liab').droplevel(0).tot_liab
        self.factor = asset / (asset - liab)
    
class DebtEquityRatio(FactorBase):
    def __init__(self):
        super().__init__('debtequityratio')
    
    def calculate(self, date):
        report_date = pq.nearest_report_period(date)[0]
        asset = pq.Stock.balance_sheet(report_date, report_date, 
            fields='tot_assets').droplevel(0).tot_assets
        liab = pq.Stock.balance_sheet(report_date, report_date,
            fields='tot_liab').droplevel(0).tot_liab
        noncurliab = pq.Stock.balance_sheet(report_date, report_date,
            fields='tot_non_cur_liab').droplevel(0).tot_non_cur_liab
        self.factor = noncurliab / (asset - liab)
    
class CashRatio(FactorBase):
    def __init__(self):
        super().__init__('cashratio')
    
    def calculate(self, date):
        report_date = pq.nearest_report_period(date)[0]
        cash = pq.Stock.balance_sheet(report_date, report_date,
            fields='monetary_cap').droplevel(0).monetary_cap
        asset = pq.Stock.balance_sheet(report_date, report_date, 
            fields='tot_assets').droplevel(0).tot_assets
        self.factor = cash / asset

class CurrentRatio(FactorBase):
    def __init__(self):
        super().__init__('currentratio')
    
    def calculate(self, date):
        report_date = pq.nearest_report_period(date)[0]
        self.factor = pq.Stock.financial_indicator(report_date, report_date,
            fields='fa_current').droplevel(0).fa_current

if __name__ == "__main__":
    factor = CurrentRatio()
    print(factor('20200106'))
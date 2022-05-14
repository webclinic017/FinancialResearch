import pandasquant as pq
from factor.define.base import FactorBase


class RoeQ(FactorBase):
    def __init__(self):
        super().__init__('roeq')
    
    def calculate(self, date):
        report_period = pq.nearest_report_period(date)[0]
        self.factor = pq.Stock.financial_indicator(report_period, report_period,
            fields='qfa_roe').droplevel(0).qfa_roe

class RoeTTM(FactorBase):
    def __init__(self):
        super().__init__('roettm')
    
    def calculate(self, date):
        self.factor = pq.Stock.pit_financial(date, date,
            fields='s_dfa_roe_ttm').droplevel(0).s_dfa_roe_ttm

class RoaQ(FactorBase):
    def __init__(self):
        super().__init__('roaq')
    
    def calculate(self, date):
        report_period = pq.nearest_report_period(date)[0]
        self.factor = pq.Stock.financial_indicator(report_period, report_period,
            fields='fa_roa2').droplevel(0).fa_roa2

class RoaTTM(FactorBase):
    def __init__(self):
        super().__init__('roattm')
    
    def calculate(self, date):
        self.factor = pq.Stock.pit_financial(date, date,
            fields='s_dfa_roa2_ttm').droplevel(0).s_dfa_roa2_ttm

class GrossProfitMarginQ(FactorBase):
    def __init__(self):
        super().__init__('grossprofitmarginq')
    
    def calculate(self, date):
        report_period = pq.nearest_report_period(date)[0]
        self.factor = pq.Stock.financial_indicator(report_period, report_period,
            fields='fa_grossmargin').droplevel(0).fa_grossmargin

class GrossProfitMarginTTM(FactorBase):
    def __init__(self):
        super().__init__('grossprofitmarginttm')
    
    def calculate(self, date):
        self.factor = pq.Stock.pit_financial(date, date,
            fields='s_dfa_grossmargin_ttm').droplevel(0).s_dfa_grossmargin_ttm

class ProfitMarginQ(FactorBase):
    def __init__(self):
        super().__init__('profitmarginq')
    
    def calculate(self, date):
        report_period = pq.nearest_report_period(date)[0]
        self.factor = pq.Stock.financial_indicator(report_period, report_period,
            fields='fa_deductedprofit').droplevel(0).fa_deductedprofit

class ProfitMarginTTM(FactorBase):
    def __init__(self):
        super().__init__('profitmarginttm')
    
    def calculate(self, date):
        self.factor = pq.Stock.pit_financial(date, date,
            fields='s_dfa_deductedprofit_ttm').droplevel(0).s_dfa_deductedprofit_ttm

class AssetTurnoverQ(FactorBase):
    def __init__(self):
        super().__init__('assetturnoverq')

    def calculate(self, date):
        report_period = pq.nearest_report_period(date)[0]
        self.factor = pq.Stock.financial_indicator(report_period, report_period,
            fields='fa_assetsturn').droplevel(0).fa_assetturnover

class AssetTurnoverTTM(FactorBase):
    def __init__(self):
        super().__init__('assetturnoverttm')
    
    def calculate(self, date):
        self.factor = pq.Stock.pit_financial(date, date,
            fields='s_dfa_currtassetstrate').droplevel(0).s_dfa_currtassetstrate

class OperationCashflowRatioQ(FactorBase):
    def __init__(self):
        super().__init__('oprationcashflowratioq')

    def calculate(self, date):
        report_period = pq.nearest_report_period(date)[0]
        ocf = pq.Stock.cashflow_sheet(report_period, report_period,
            fields='net_cash_flows_oper_act').droplevel(0).net_cash_flows_oper_act
        np = pq.Stock.income_sheet(report_period, report_period,
            fields='net_profit_excl_min_int_inc').droplevel(0).net_profit_excl_min_int_inc
        self.factor = ocf / np

class OperationCashflowRatioTTM(FactorBase):
    def __init__(self):
        super().__init__('oprationcashflowratiottm')
    
    def calculate(self, date):
        ocf = pq.Stock.pit_financial(date, date,
            fields='s_dfa_operatecashflow_ttm').droplevel(0).s_dfa_operatecashflow_ttm
        np = pq.Stock.pit_financial(date, date,
            fields='s_dfa_profit_ttm').droplevel(0).s_dfa_profit_ttm
        self.factor = ocf / np


if __name__ == "__main__":
    factor = OperationCashflowRatioTTM()
    print(factor('20200106'))
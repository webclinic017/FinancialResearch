import pandas as pd
import pandasquant as pq


class FactorBase(object):
    '''Base Class for all factors to be defined
    ============================================

    There are five steps to define a factor:

        1. Define the class inheriting FactorBase
        
        2. rewrite `basic_info` according to your needs, this
            method provides basic information like stocks
            in stock pool and corresponding industry
        
        3. rewrite `calcuate_factor` according to your needs,
            this method can provide the factor value in a series form
            
        4. rewrite `factor_process` according to your needs,
            the process inculdes standardization, deextreme, missing fill

        5. rewrite `factor_modify` according to your needs,
            this method adjusted the index into multiindex form and rename
    '''
    def __init__(self, name):
        self.name = name

    def info(self, date):
        self.stocks = pq.Stock.index_weight(date, date, fields='code', 
            and_='index_code="000300.SH"').code.to_list()

    def calculate(self, date):
        raise NotImplementedError

    def process(self, date):
        grouper = pq.Stock.plate_info(date, date, fields='citi_industry_name1')\
            .citi_industry_name1.droplevel(0)
        self.factor = self.factor.loc[self.factor.index.intersection(self.stocks)]
        self.factor = self.factor.preprocessor.deextreme(method='mad', grouper=grouper)\
            .preprocessor.standarize(method='zscore', grouper=grouper)\
            .preprocessor.fillna(method='mean', grouper=grouper)
    
    def modify(self, date):
        self.factor.index = pd.MultiIndex.from_product([[pq.str2time(date)], self.factor.index])
        self.factor.index.names = ["date", "asset"]
        self.factor.columns = [self.name]

    def __call__(self, date) -> ...:
        self.info(date)
        self.calculate(date)
        self.process(date)
        self.modify(date)
        return self.factor
    
    def __str__(self) -> str:
        return self.name
    
    def __repr__(self) -> str:
        return self.name

import pandas as pd
import statsmodels.api as sm
from factor.utils.prepare import *

def regression(data: pd.DataFrame) -> pd.DataFrame:
    '''Calculate factor regression results
    --------------------------------------

    data: pd.DataFrame, data must be in the standard format
        generated by function `factor_datas_and_forward_returns`
        in factor/utils/prepare.py
    return: pd.DataFrame, a dataframe with multi-index and columns
        one is t value for each regression; another is coefficient
    '''
    def _reg(d):
        grp = pd.get_dummies(d.loc[:, 'group']).iloc[:, :-1]
        x = pd.concat([d.loc[:, factor], grp], axis=1)
        y = d.loc[:, period]
        x = sm.add_constant(x)
        model = sm.OLS(y, x).fit()
        t = model.tvalues[factor]
        coef = model.params[factor]
        return pd.Series({f't_{period}': t, f'coef_{period}': coef})
    
    factors = get_factor_columns(data)
    forward = get_forward_return_columns(data)

    results = pd.DataFrame()
    for factor in factors:
        period_result = []
        for period in forward:
            tmp_data = data.loc[:, [factor, 'group', period]].copy()
            result = tmp_data.groupby(level='date').apply(_reg)
            result.index = pd.MultiIndex.from_product(
                [[factor], result.index], names=['factor', 'date'])
            period_result.append(result)
        results = results.append(pd.concat(period_result, axis=1))
    return results

def ic(data: pd.DataFrame) -> pd.DataFrame:
    '''Calculate factor ic results
    --------------------------------------

    data: pd.DataFrame, data must be in the standard format
        generated by function `factor_datas_and_forward_returns`
        in factor/utils/prepare.py
    return: pd.DataFrame, a dataframe with multi-index and columns
    '''
    def _ic(d):
        res = d.corr(method='spearman')
        val = res.loc[factor, period]
        return pd.Series({f"ic_{period}": val})
        
    factors = get_factor_columns(data)
    forward = get_forward_return_columns(data)

    results = pd.DataFrame()
    for factor in factors:
        period_result = []
        for period in forward:
            tmp_data = data.loc[:, [factor, period]]
            result = tmp_data.groupby(level='date').apply(_ic)
            result.index = pd.MultiIndex.from_product(
                [[factor], result.index], names=['factor', 'date'])
            period_result.append(result)
        results = results.append(pd.concat(period_result, axis=1))
    return results
    
def layering(data: pd.DataFrame, quantiles: int = 5) -> pd.DataFrame:
    '''Calculate factor layering results
    --------------------------------------

    data: pd.DataFrame, data must be in the standard format
        generated by function `factor_datas_and_forward_returns`
        in factor/utils/prepare.py
    return: pd.DataFrame, a dataframe with multi-index and columns
    '''
    def _layer(d):
        index = d.index.levels[0]
        start = index[0]
        index = index.map(lambda x: next_n_trade_dates(x, 1))
        index = index.insert(0, start)
        prft = d[period].groupby(level='date').mean()
        prft = pd.Series([0] + prft.to_list(), index=index, name=f'profit_{period}')
        cum_prft = (prft + 1).cumprod()
        cum_prft.name = f'cum_profit_{period}'
        result = pd.concat([cum_prft, prft], axis=1)
        return result
    
    factors = get_factor_columns(data)
    forward = get_forward_return_columns(data)

    results = pd.DataFrame()
    for factor in factors:
        period_result = []
        for period in forward:
            tmp_data = data.loc[:, [factor, period]]
            tmp_data['quantiles'] = tmp_data[factor].groupby(level='date').apply(
                pd.qcut, q=quantiles, labels=range(1, quantiles + 1))
            result = tmp_data.groupby(by='quantiles').apply(_layer)
            result.index = pd.MultiIndex.from_arrays(
                [[factor] * len(result), result.index.get_level_values(0), result.index.get_level_values(1)],
                names = ['factor', 'quantiles', 'date'])
            period_result.append(result)
        results = results.append(pd.concat(period_result, axis=1))
    return results

if __name__ == "__main__":
    factors = ['return_1m', 'return_3m']
    dates = trade_date('2011-02-01', '2011-12-31', freq='monthly')
    forward_period = [1, 3]
    data = factor_datas_and_forward_returns(factors, dates, forward_period)
    reg_results = regression(data)
    ic_results = ic(data)
    layer_result = layering(data)
    print(reg_results)
    print(ic_results)
    print(layer_result)
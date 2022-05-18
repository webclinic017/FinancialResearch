# --- 导入依赖
import pandasquant as pq
import pandas as pd
from scipy.stats import norm

# --- 需要的数据列表
relation = pd.read_excel('Researches/FinancialRiskRatio/relation.xlsx').dropna()
relation_list = set(relation['from'].to_list() + relation['to'].to_list())
balance_indicator = list(map(lambda x: x.split('.')[1], list(filter(lambda x: x.startswith('balance'), relation_list))))
income_indicator = list(map(lambda x: x.split('.')[1] , list(filter(lambda x: x.startswith('income'), relation_list))))
cashflow_indicator = list(map(lambda x: x.split('.')[1], list(filter(lambda x: x.startswith('cashflow'), relation_list))))
ttm_indicator = list(map(lambda x: x.split('.')[1], list(filter(lambda x: x.startswith('ttm'), relation_list))))

# --- 读取必要的数据
industry = pd.read_parquet('Researches/FinancialRiskRatio/data.nosync/industry.parquet')
ttm = pd.read_parquet('Researches/FinancialRiskRatio/data.nosync/ttm.parquet')
balance = pd.read_parquet('Researches/FinancialRiskRatio/data.nosync/balance.parquet')
income = pd.read_parquet('Researches/FinancialRiskRatio/data.nosync/income.parquet')
cashflow = pd.read_parquet('Researches/FinancialRiskRatio/data.nosync/cashflow.parquet')

# --- ttm数据及industry数据需要对交易日进行调整
ttm = ttm.converter.resample('q').last()
industry = industry.converter.resample('q').last()

# --- select the common part
common_index = balance.index.levels[0].intersection(income.index.levels[0]).\
    intersection(cashflow.index.levels[0]).intersection(ttm.index.levels[0])
balance = balance.loc[common_index]
income = income.loc[common_index]
cashflow = cashflow.loc[common_index]
ttm = ttm.loc[common_index]
industry = industry.loc[common_index]
# rename the index
balance.index.names = ['datetime', 'asset']
cashflow.index.names = ['datetime', 'asset']
income.index.names = ['datetime', 'asset']
ttm.index.names = ['datetime', 'asset']
industry.index.names = ['datetime', 'asset']

# --- 对利润表数据进行ttm处理
income = income.converter.cum2diff(grouper=lambda x: x[0].year)
income = income.groupby(level=1).rolling(4, min_periods=1).sum().droplevel(0)

# --- 提取净利润
net_profit = ttm.loc[:, 'net_profit_ttm']
net_profit_forward = net_profit.converter.price2fwd(1)
net_profit_industry = net_profit.groupby([pd.Grouper(level=0), industry.group]).mean()
net_profit_industry_forward = net_profit_industry.converter.price2fwd(1)
net_profit_industry_forward = net_profit_industry_forward.preprocessor.standarize(method='zscore')

# --- 计算比值
ratio = pd.DataFrame()
for i, rel in relation.iterrows():
    from_table = eval(rel['from'].split('.')[0])
    from_col = rel['from'].split('.')[1]
    from_var = from_table[from_col]
    from_var = from_var[~from_var.index.duplicated(keep='first')]
    to_table = eval(rel['to'].split('.')[0])
    to_col = rel['to'].split('.')[1]
    to_var = to_table[to_col]
    to_var = to_var[~to_var.index.duplicated(keep='first')]
    ratio[i] = from_var / to_var

# 尝试减少数据量
ratio = ratio[ratio.columns[(ratio.count() / ratio.shape[0]) > 0.5]]
# 数据清洗
ratio_processed = ratio.preprocessor.standarize('zscore')\
    .preprocessor.fillna('mean', grouper=industry.group)\
    .preprocessor.deextreme('mad')

# --- 滚动窗口计算行业内的财务质量得分
def calc_unit(data):
    def _calc_inner_ind(_data):
        res = 1 - norm.sf(_data) * 2
        res = pd.DataFrame(res, index=_data.index, columns=_data.columns)
        return res.groupby(level=1).last().mean(axis=1)

    # the latest industry infomation
    idx = data.index.get_level_values(0)[-1]
    currect_group = industry.loc[idx].group.to_dict()

    dev = data.groupby(lambda x: currect_group.get(x[1], '')).apply(_calc_inner_ind)
    score = dev.abs().groupby(level=1).last()
    return score

score = ratio_processed.calculator.rolling(window=30, func=calc_unit)

# --- 板块分类器
plate_dict = {
    '600': 'zb',
    '601': 'zb',
    '603': 'zb',
    '605': 'zb',
    '000': 'zb',
    '002': 'zxb',
    '300': 'cyb',
    '688': 'kcb',
}

# ---
ic = score.describer.ic(net_profit_forward)
# ic = score.describer.ic(net_profit_forward, grouper=industry.group)
# ic = score.describer.ic(net_profit_forward, grouper=lambda x: plate_dict.get(x[1][:3], 'unknown'))
test_result = ic.tester.sigtest()
# ir = ic.groupby(level=1).mean() / ic.groupby(level=1).std()
# ir = ic.mean() / ic.std()

# ---
with pq.Gallery(1, 1, path='Researches/FinancialRiskRatio/image/ic-all.png') as g:
    ic.drawer.draw('bar', indicator='factor', ax=g.axes[0, 0], title='all_stock', label='corr')
    ic.rolling(4).mean().drawer.draw('line', indicator='factor', 
        label='MA_Year', ax=g.axes[0, 0], color='#CC6633')
    g.axes[0, 0].legend()

# ---
for plate in ['zb', 'cyb', 'kcb', 'zxb']:
    with pq.Gallery(1, 1, path=f'Researches/FinancialRiskRatio/image/ic-by-{plate}.png') as g:
        ic.drawer.draw('bar', asset=f'{plate}', indicator='factor', 
            ax=g.axes[0, 0], title=f'{plate}', label='corr')
        ic.groupby(level=1).rolling(4).mean().droplevel(0).sort_index().drawer.draw(
            'line', asset=f'{plate}', indicator='factor', label='MA_Year', ax=g.axes[0, 0], color='#CC6633')
        g.axes[0, 0].legend()

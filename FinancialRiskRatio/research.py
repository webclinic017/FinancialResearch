# --- 导入依赖
import pandasquant as pq
from scipy.stats import norm

# --- 需要的数据列表
relation = pq.read_excel('FinancialRiskRatio/relation.xlsx').dropna()
relation_list = set(relation['from'].to_list() + relation['to'].to_list())
balance_indicator = list(map(lambda x: x.split('.')[1], list(filter(lambda x: x.startswith('balance'), relation_list))))
income_indicator = list(map(lambda x: x.split('.')[1] , list(filter(lambda x: x.startswith('income'), relation_list))))
cashflow_indicator = list(map(lambda x: x.split('.')[1], list(filter(lambda x: x.startswith('cashflow'), relation_list))))
ttm_indicator = list(map(lambda x: x.split('.')[1], list(filter(lambda x: x.startswith('ttm'), relation_list))))

# --- 读取必要的数据
industry = pq.read_parquet('FinancialRiskRatio/data.nosync/industry.parquet')
ttm = pq.read_parquet('FinancialRiskRatio/data.nosync/ttm.parquet')
balance = pq.read_parquet('FinancialRiskRatio/data.nosync/balance.parquet')
income = pq.read_parquet('FinancialRiskRatio/data.nosync/income.parquet')
cashflow = pq.read_parquet('FinancialRiskRatio/data.nosync/cashflow.parquet')

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

# --- 计算比值
ratio = pq.DataFrame()
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

# --- 滚动窗口计算行业内的财务质量得分
def calc_unit(data):
    # deextreme, normalization
    data = data.preprocessor.deextreme('md_correct').preprocessor.standarize("zscore")
    dev = pq.DataFrame(1 - norm.sf(data) * 2, index=data.index, columns=data.columns)
    score = dev.abs().mean().mean()
    return score

score = ratio.calculator.rolling(window=4, func=calc_unit, grouper=industry['group'])

# ---
(score - 0.5).drawer.draw('bar', datetime='20200331')

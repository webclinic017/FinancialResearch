# --- 倒入必要的包
import pandasquant as pd
import numpy as np
from scipy.stats import norm

# --- 读取要使用的数据配置列表
index = pd.read_excel('other/financial_ratio/index_list.xlsx', usecols=['fromgs', 'togs'])

# --- all_index是所有要用到的指标
all_index = index['fromgs'].dropna().unique().tolist() + index['togs'].dropna().unique().tolist()

# --- 从所有用到的指标里面筛选出
income_index = set(filter(lambda x: x.startswith('income'), all_index))
balance_index = set(filter(lambda x: x.startswith('balance'), all_index))
cashflow_index = set(filter(lambda x: x.startswith('cashflow'), all_index))
income_col = list(map(lambda x: x.split('.')[1], income_index)) + ['statement_type', 'stock_id', 'report_date']
balance_col = list(map(lambda x: x.split('.')[1], balance_index)) + ['statement_type', 'stock_id', 'report_date']
cashflow_col = list(map(lambda x: x.split('.')[1], cashflow_index)) + ['statement_type', 'stock_id', 'report_date']

# --- 读取的是需要的财务指标相关数据
balance = pd.read_csv('assets/data/financial_report/balance/total.csv', 
    usecols=balance_col, index_col=[0, 1], parse_dates=True)
income = pd.read_csv('assets/data/financial_report/income/total.csv',
    usecols=income_col, index_col=[1, 0], parse_dates=True).sort_index()
cashflow = pd.read_csv('assets/data/financial_report/cashflow/total.csv',
    usecols=cashflow_col, index_col=[0, 1], parse_dates=True)

# --- 取合并报表的部分
balance = balance.loc[balance.statement_type == 408001000].drop('statement_type', axis=1)
income = income.loc[income.statement_type == 408001000].drop('statement_type', axis=1)
cashflow = cashflow.loc[cashflow.statement_type == 408001000].drop('statement_type', axis=1)

# --- 限制时间为2010年到2021年
daterange = pd.date_range('20100101', '20211231', freq='q')
balance = balance.loc[daterange]
income = income.loc[daterange]
cashflow = cashflow.loc[daterange]

balance.index = balance.index.remove_unused_levels()
income.index = income.index.remove_unused_levels()
cashflow.index = cashflow.index.remove_unused_levels()

cashflow = cashflow[~cashflow.index.duplicated(keep='first')]
income = income[~income.index.duplicated(keep='first')]
balance = balance[~balance.index.duplicated(keep='first')]

# ---- 获取每个季度末的股票的行业分类（这里必须特别注意，行业分类频率和财报时间频率不同）
stock_status = pd.read_csv('assets/data/stock/status/industry.csv', index_col=[0, 1], parse_dates=True)
stock_status = stock_status.preprocessor.resample('q').last()
stock_status = stock_status.loc[(daterange, slice(None)), slice(None)]
stock_status.index.names = ['', '']

# --- 计算时间段的财务指标发生值，而非使用累积值
income_indicator = income.preprocessor.cum2diff(grouper=lambda x: x[0].year)
income_indicator.index.names = ['', '']
balance_indicator = balance
balance_indicator.index.names = ['', '']
cashflow_indicator = cashflow.preprocessor.cum2diff(grouper=lambda x: x[0].year)
cashflow_indicator.index.names = ['', '']

# --- 计算财务指标之间的比值
indicators = []
for idx in index.dropna().index:
    from_var = eval(index.loc[idx, 'fromgs'].split('.')[0] + "_indicator")
    to_var = eval(index.loc[idx, 'togs'].split('.')[0] + "_indicator")
    from_name = index.loc[idx, 'fromgs'].split('.')[1]
    to_name = index.loc[idx, 'togs'].split('.')[1]
    indi = (from_var[from_name] / to_var[to_name]).replace(np.inf, np.nan).replace(-np.inf, np.nan)
    indi.name = from_name + '/' + to_name
    indicators.append(indi)
    
# --- 合并指标以供后续计算
indicators = pd.concat(indicators, axis=1)
        
# --- 定义计算单元并对滚动窗口进行计算
def calculate_unit(data):
    mean = data.mean(axis=0)
    std = data.std(axis=0)
    data_normalized = (data - mean) / std
    score = pd.DataFrame(norm.sf(data_normalized) - 0.5, index=data.index, columns=data.columns)
    score = score.abs().mean()
    score = pd.DataFrame([score], index=[data.index.get_level_values(0)[-1]], columns=data.columns)
    return score
    
result = indicators.calculator.rolling(window=4, grouper=stock_status.group, func=calculate_unit)

# --- 读取行业名称与行业代码对应表
names = pd.read_excel('assets/data/industry/citics_name.xlsx', index_col="行业代码").loc[:, "行业简称"].to_dict()
price = pd.read_csv('assets/data/industry/citics_close.csv', index_col=0, parse_dates=True)
price = price.rename(columns=names).drop(['CI005030.WI', 'CI005029.WI'], axis=1)

# --- 统一行业指标结果与价格数据的频率
ret = price.preprocessor.price2fwd('q').loc[daterange]

# --- 绘图
with pd.Gallery(len(ret.columns[:-2]), 1, path='test.png') as (_, axes):
    for i, col in enumerate(ret.columns[:-2]):
        ax = axes[i, 0]
        result.drawer.draw(kind='line', indicator='score', asset=col, 
            ax=ax, legend=True, label='indicator', color='red')
        ret[[col]].drawer.draw(kind='bar', ax=ax.twinx(), legend=True,
            label='3m forward return', title=col, color='green')

# --- 计算一下IC
result_ = pd.concat([result, ret.stack()], axis=1)
result_.columns = ['score', 'ret']
ic = result_.describer.corr().loc[(slice(None), "score"), "ret"].droplevel(1)


# ---
ttm = pd.Filer.read_csv_directory('assets/data/financial_report/ttm/', perspective='datetime', usecols=range(1, 20), index_col=[0,1], parse_dates=True)
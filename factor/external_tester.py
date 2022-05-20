# ---
import pandas as pd
import numpy as np
import pandasquant as pq
from factor.tools import factor_analysis, process_factor

# ---
open_price = pq.Stock.market_daily('20210101', '20211204', fields='open')
forward_return = open_price.groupby(
    level=1).shift(-1) / open_price.groupby(level=1).shift(-2) - 1
industry = pq.Stock.plate_info(
    '20210101', '20211204', fields='citi_industry_name1')

# --- improved momentum
@process_factor(name='momentum')
def improved_momentum():
    morning_datas = []
    afternoon_datas = []
    for month in ['202101', '202102', '202103', '202104', '202105', '202106', '202107', '202108', '202109', '202110', '202111']:
        data = pd.read_parquet(f'data.nosync/{month}')
        morning_datas.append(
            data.iloc[data.index.get_level_values(1).indexer_at_time('10:00:00'), 0])
        afternoon_datas.append(
            data.iloc[data.index.get_level_values(1).indexer_at_time('15:00:00'), 0])
    morning_data = pd.concat(morning_datas)
    afternoon_data = pd.concat(afternoon_datas)
    morning_data.index = pd.MultiIndex.from_arrays([morning_data.index.get_level_values(
        0), pd.to_datetime(morning_data.index.get_level_values(1).date)])
    afternoon_data.index = pd.MultiIndex.from_arrays([afternoon_data.index.get_level_values(
        0), pd.to_datetime(afternoon_data.index.get_level_values(1).date)])
    factor = (afternoon_data / morning_data - 1)
    factor = factor.swaplevel().sort_index()
    factor.index = pd.MultiIndex.from_arrays([factor.index.get_level_values(0),
                                            factor.index.get_level_values(1).map(lambda x: x.split('.')[0] + ('.SZ' if x.split('.')[-1] == 'XSHE' else '.SH'))])
    return factor

# --- ret std
@process_factor(name='retstd')
def ret_std():
    factors = []
    for month in ['202101', '202102', '202103', '202104', '202105', '202106', '202107', '202108', '202109', '202110', '202111']:
        data = pd.read_parquet(f'data.nosync/{month}')
        logret = np.log(data['close'] / data['open'])
        logret2 = logret.pow(2)
        factors.append(logret2.groupby(
            [pd.Grouper(level=0), logret.index.get_level_values(1).date]).sum())
    factor = pd.concat(factors)
    factor = factor.swaplevel().sort_index()
    factor.index = pd.MultiIndex.from_arrays([pd.to_datetime(factor.index.get_level_values(0)),
                                            factor.index.get_level_values(1).map(lambda x: x.split('.')[0] + ('.SZ' if x.split('.')[-1] == 'XSHE' else '.SH'))])
    return factor

# --- volume portion during last 30 minutes
@process_factor(name='volumeportion')
def volume_portion():
    factors = []
    for month in ['202101', '202102', '202103', '202104', '202105', '202106', '202107', '202108', '202109', '202110', '202111']:
        data = pd.read_parquet(f'data.nosync/{month}')
        volume = data['volume']
        last_volume = volume.iloc[volume.index.get_level_values(
            1).indexer_between_time('14:30:00', '15:00:00')]
        last_volume = last_volume.groupby([pd.Grouper(level=0), last_volume.index.get_level_values(1).date
            ]).sum().swaplevel().sort_index()
        volume = volume.groupby([pd.Grouper(level=0), volume.index.get_level_values(1).date]
            ).sum().swaplevel().sort_index()
        factors.append(last_volume / volume)

    factor = pd.concat(factors, axis=0)
    factor.index = pd.MultiIndex.from_arrays([pd.to_datetime(factor.index.get_level_values(0)),
                                            factor.index.get_level_values(1).map(lambda x: x.split('.')[0] + ('.SZ' if x.split('.')[-1] == 'XSHE' else '.SH'))])
    return factor

# ---
factor = improved_momentum()
factor_analysis(factor, forward_return.open, industry.citi_industry_name1, 
    datapath='result.nosync/factor-test.xlsx', savedata=['reg', 'ic', 'layering', 'data'])

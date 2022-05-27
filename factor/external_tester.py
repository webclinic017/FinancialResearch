import datetime
import pandas as pd
import numpy as np
import pandasquant as pq
from factor.tools import single_factor_analysis, process_factor


@process_factor(name='pvcorr')
def pvcorr(price, period):
    vwap = price.amount / price.volume
    vwap.name = 'vwap'
    data = pd.concat([vwap, price.volume], axis=1)
    factor = data.calculator.rolling(window=period, func=
        lambda x: x.groupby(level=1).corr().loc[(slice(None), 'vwap'), 'volume'].droplevel(1))
    return factor

if __name__ == "__main__":
    today = datetime.datetime.today()
    last_date = today - datetime.timedelta(days=365)

    price = pq.Api.market_daily(start=last_date, end=today, 
        fields=['n_adjClose', 'n_volume', 'n_amount'])
    forward_return = price.converter.price2fwd(period=1, 
        open_col='adj_close', close_col='adj_close')
    industry = pq.Api.plate_info(start=last_date, end=today, 
        fields='c_citiIndustryName1').iloc[:, 0]
    industry = industry.loc[~industry.index.duplicated(keep='first')]
    factor = pvcorr(price, period=20)

    single_factor_analysis(factor, forward_return, industry, commission=0.001,
        data_path=f'result.nosync/table/{factor.name}.xlsx', 
        image_path=f'result.nosync/image/{factor.name}')

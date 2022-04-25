import os
import datetime
import pandas as pd
from ..tools import *
from functools import lru_cache
from sqlalchemy.pool import NullPool
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool


@pd.api.extensions.register_dataframe_accessor("filer")
class Filer(Worker):
    
    def to_multisheet_excel(self, path, **kwargs):
        if self.type_ == Worker.PANEL:
            with pd.ExcelWriter(path) as writer:
                for column in self.dataframe.columns:
                    self.dataframe[column].unstack(level=1).to_excel(writer, sheet_name=str(column), **kwargs)
        
        else:
            self.dataframe.to_excel(path, **kwargs)

    @staticmethod
    def read_excel(path, perspective: str = None, **kwargs):
        '''A dummy function of pd.read_csv, which provide multi sheet reading function'''
        if perspective is None:
            return pd.read_excel(path, **kwargs)
        
        sheets_dict = pd.read_excel(path, sheet_name=None, **kwargs)
        datas = []
        if perspective == "indicator":
            for indicator, data in sheets_dict.items():
                data = data.stack()
                data.name = indicator
                datas.append(data)
            datas = pd.concat(datas, axis=1)

        elif perspective == "asset":
            for asset, data in sheets_dict.items():
                data.index = pd.MultiIndex.from_product([data.index, [asset]])
                datas.append(data)
            datas = pd.concat(datas)
            datas = data.sort_index()

        elif perspective == "datetime":
            for datetime, data in sheets_dict.items():
                data.index = pd.MultiIndex.from_product([[datetime], data.index])
                datas.append(data)
            datas = pd.concat(datas)

        else:
            raise ValueError('perspective must be in one of datetime, indicator or asset')
        
        return datas

    @staticmethod
    def read_csv_directory(path, perspective: str, **kwargs):
        '''A enhanced function for reading files in a directory to a panel DataFrame
        ----------------------------------------------------------------------------

        path: path to the directory
        perspective: 'datetime', 'asset', 'indicator'
        kwargs: other arguments for pd.read_csv

        **note: the name of the file in the directory will be interpreted as the 
        sign(column or index) to the data, so set it to the brief one
        '''
        files = os.listdir(path)
        datas = []
        
        if perspective == "indicator":
            for file in files:
                name = os.path.splitext(file)[0]
                data = pd.read_csv(os.path.join(path, file), **kwargs)
                data = data.stack()
                data.name = name
                datas.append(data)
            datas = pd.concat(datas, axis=1).sort_index()

        elif perspective == "asset":
            for file in files:
                name = os.path.splitext(file)[0]
                data = pd.read_csv(os.path.join(path, file), **kwargs)
                data.index = pd.MultiIndex.from_product([data.index, [name]])
                datas.append(data)
            datas = pd.concat(datas).sort_index()
            
        elif perspective == "datetime":
            for file in files:
                name = os.path.splitext(file)[0]
                data = pd.read_csv(os.path.join(path, file), **kwargs)
                data.index = pd.MultiIndex.from_product([pd.to_datetime([name]), data.index])
                datas.append(data)
            datas = pd.concat(datas).sort_index()
        
        return datas

    @staticmethod
    def read_excel_directory(path, perspective: str, **kwargs):
        '''A enhanced function for reading files in a directory to a panel DataFrame
        ----------------------------------------------------------------------------

        path: path to the directory
        perspective: 'datetime', 'asset', 'indicator'
        kwargs: other arguments for pd.read_excel

        **note: the name of the file in the directory will be interpreted as the 
        sign(column or index) to the data, so set it to the brief one
        '''
        files = os.listdir(path)
        datas = []
        
        if perspective == "indicator":
            for file in files:
                name = os.path.splitext(file)[0]
                data = pd.read_excel(os.path.join(path, file), **kwargs)
                data = data.stack()
                data.name = name
                datas.append(data)
            datas = pd.concat(datas, axis=1).sort_index()

        elif perspective == "asset":
            for file in files:
                name = os.path.splitext(file)[0]
                data = pd.read_excel(os.path.join(path, file), **kwargs)
                data.index = pd.MultiIndex.from_product([data.index, [name]])
                datas.append(data)
            datas = pd.concat(datas).sort_index()
            
        elif perspective == "datetime":
            for file in files:
                name = os.path.splitext(file)[0]
                data = pd.read_excel(os.path.join(path, file), **kwargs)
                data.index = pd.MultiIndex.from_product([pd.to_datetime([name]), data.index])
                datas.append(data)
            datas = pd.concat(datas).sort_index()
        
        return datas

class Database(object):

    def __init__(self, user: str, password: str):
        Database.user = user
        Database.password = password

        base = "mysql+pymysql://{user}:{password}@127.0.0.1/{database}?charset=utf8"
        Database.stock = base.format(user=user, password=password, database="stock")
        Database.fund = base.format(user=user, password=password, database="fund")
        Database.factor = base.format(user=user, password=password, database="factor")
    
    @classmethod
    def trade_date(cls, start: 'datetime.date | datetime.datetime | str',
        end: 'datetime.date | datetime.datetime | str',
        freq: str = 'daily') -> list[datetime.date]:
        '''get trade date
        ------------------

        start: datetime or date or str, start date in 3 forms
        end: datetime or date or str, end date in 3 forms
        freq: str, frequency in either 'daily', 'weekly' or 'monthly'
        '''
        start = time2str(start)
        end = time2str(end)
        data = pd.read_sql(f'select trade_date from trade_date_{freq} ' + \
            f'where trade_date >= "{start}" ' + \
            f'and trade_date <= "{end}"', cls.stock)
        data = data.trade_date.sort_values().tolist()
        return data

    @classmethod
    def trade_weekday(cls, start_date: 'str | datetime.datetime',
                    end_date: 'str | datetime.datetime',
                    weekday: int) -> list[datetime.date]:
      '''get trade weekday
      --------------------

      start_date: str or datetime, start date
      end_date: str or datetime, end date
      weekday: int, specific weekday
      type: str, type of market (szse 深圳交易所; sse 上证交易所; szn 中国期货市场)
      return: a list of trading dates between start date and end date
      '''
      start_date = time2str(start_date)
      end_date = time2str(end_date)
      sql = f'select * from `trade_date_daily` ' + \
            f'where `trade_date` >= "{start_date}" ' + \
            f'and `trade_date` <= "{end_date}" ' + \
            f'and `weekday` = {weekday}'

      data = pd.read_sql(sql, cls.stock)
      data = data.drop(['weekday'], axis=1)
      data = data.trade_date.sort_values().tolist()
      return data

    @classmethod
    def trade_date_m(cls, start_date: 'str | datetime.datetime',
                    end_date: 'str | datetime.datetime') -> list[datetime.date]:
        '''Update data for table stock.trad_date_monthly
        -----------------------------------------------

        start_date: str or datetime, start date
        end_date: str or datetime, end date
        type: str, type of market (szse 深圳交易所; sse 上证交易所; szn 中国期货市场)
        return: a list of trading dates between start date and end date
        '''
        start_date = time2str(start_date)
        end_date = time2str(end_date)
        sql = f'select * from `trade_date_daily` ' + \
              f'where `trade_date` >= "{start_date}" ' + \
              f'and `trade_date` <= "{end_date}" '
        data = pd.read_sql(sql, cls.stock)
        data = data.iloc[[i for i in range(0, len(data), 22)]]
        data = data.drop(['weekday'], axis=1)
        data = data.trade_date.sort_values().tolist()
        return data

    @classmethod
    def market_daily(cls, start: 'datetime.date | str | datetime.datetime',
                    end: 'str | datetime.date | datetime.datetime',
                    fields: list = None,
                    conditions: list = None) -> pd.DataFrame:
        '''get market data in daily frequency
        -------------------------------------

        start: datetime or date or str, start date in 3 forms
        end: datetime or date or str, end date in 3 forms
        fields: list, the field names you want to get
        conditions: list, a series of conditions like "code = '000001.SZ'" listed in a list
        '''
        start = time2str(start)
        end = time2str(end)
        # get data
        if fields is None:
            fields = '*'
        else:
            fields = [f'`{i}`' for i in fields]
            fields = ', '.join(fields)
        sql = f'select {fields} from market_daily where trade_date >= "{start}" and trade_date <= "{end}"'
        if conditions:
            conditions = 'and ' + 'and'.join(conditions)
            sql += conditions
        data = pd.read_sql(sql, cls.stock)

        # modify time format
        if 'trade_date' in fields:
            data.trade_date = pd.to_datetime(data.trade_date)

        # modify dataframe index
        index = ['trade_date', 'code']
        index = list(filter(lambda x: x in fields, index))
        if index:
            data = data.set_index(index)
        return data

    def market_daily_single(start_date: 'datetime.date | str | datetime.datetime',
        end_date: 'datetime.date | str | datetime.datetime',
        code: str) -> pd.DataFrame:
        """
        获取某只股票某一时段的行情
        start_date, date
        end_date, date,
        code, str
        """
        start_date = time2str(start_date)
        end_date = time2str(end_date)
        sql = f'select trade_date, open, high, low, close, volume ' + \
            f'from market_daily ' + \
            f'where trade_date >= "{start_date}" ' +\
            f'and trade_date <= "{end_date}" ' + \
            f'and code = "{code}"'
        data = pd.read_sql(sql, stock)
        data = data.set_index(['trade_date'])
        data.index = pd.DatetimeIndex(data.index)
        return data

    def index_market_daily(code: str,
                        start: Union[datetime.date, str, datetime.datetime],
                        end: Union[str, datetime.date, datetime.datetime],
                        fields: list = None,
                        conditions: list = None) -> pd.DataFrame:
        '''get index market data in daily frequency
        -------------------------------------

        start: datetime or date or str, start date in 3 forms
        end: datetime or date or str, end date in 3 forms
        code: str, the index code
        fields: list, the field names you want to get
        conditions: list, a series of conditions like "code = '000001.SZ'" listed in a list
        '''
        start = time2str(start)
        end = time2str(end)
        # get data
        if fields is None:
            fields = '*'
        else:
            fields = ', '.join(fields)
        sql = f'select {fields} from index_market_daily ' + \
            f'where `index_code` = "{code}" and ' + \
            f'trade_date >= "{start}" and trade_date <= "{end}"'
        if conditions:
            conditions = 'and ' + 'and'.join(conditions)
            sql += conditions
        data = pd.read_sql(sql, stock)

        # modify time format
        if 'trade_dt' in fields:
            data.trade_dt = pd.to_datetime(data.trade_dt)

        # modify dataframe index
        index = ['trade_dt', 's_info_windcode']
        index = list(filter(lambda x: x in fields, index))
        if index:
            data = data.set_index(index)
        return data

    def plate_info(start: Union[datetime.datetime, datetime.date, str],
                end: Union[datetime.datetime, datetime.date, str],
                fields: list = None,
                conditions: list = None) -> pd.DataFrame:
        '''get plate info in daily frequency
        -------------------------------------

        start: datetime or date or str, start date in 3 forms
        end: datetime or date or str, end date in 3 forms
        fields: list, the field names you want to get
        conditions: list, a series of conditions like "code = '000001.SZ'" listed in a list
        '''
        start = time2str(start)
        end = time2str(end)
        # get data
        if fields is None:
            fields = '*'
        else:
            fields = ', '.join(fields)
        sql = f'select {fields} from plate_info ' + \
            f'where trade_date >= "{start}" and trade_date <= "{end}"'
        if conditions:
            conditions = 'and ' + 'and'.join(conditions)
            sql += conditions
        data = pd.read_sql(sql, stock)

        # modify time format
        if 'trade_date' in fields:
            data.trade_date = pd.to_datetime(data.trade_date)
        
        # modify dataframe index
        index = ['trade_date', 'code']
        index = list(filter(lambda x: x in fields, index))
        if index:
            data = data.set_index(index)
        return data

    def derivative_indicator(start: Union[datetime.datetime, datetime.date, str],
                        end: Union[datetime.datetime, datetime.date, str],
                        fields: list = None,
                        conditions: list = None) -> pd.DataFrame:
        '''get derivative indicator in daily frequecy
        ---------------------------------------------

        start: datetime or date or str, start date in 3 forms
        end: datetime or date or str, end date in 3 forms
        fields: list, the field names you want to get
        conditions: list, a series of conditions like "code = '000001.SZ'" listed in a list
        '''
        start = time2str(start)
        end = time2str(end)
        # get data
        if fields is None:
            fields = '*'
        else:
            fields = ', '.join(fields)
        sql = f'select {fields} from derivative_indicator ' + \
            f'where trade_date >= "{start}" and trade_date <= "{end}"'
        if conditions:
            conditions = 'and ' + 'and'.join(conditions)
            sql += conditions
        data = pd.read_sql(sql, stock)

        # modify time format
        if 'trade_dt' in fields:
            data.trade_dt = pd.to_datetime(data.trade_dt)
        
        # modify dataframe index
        index = ['trade_dt', 's_info_windcode']
        index = list(filter(lambda x: x in fields, index))
        if index:
            data = data.set_index(index)
        return data
    
    def active_opdep(date: Union[datetime.datetime, datetime.date, str]) -> pd.DataFrame:
        """
        返回龙虎榜数据
        date: 日期
        return: 返回dataframe
        """
        date = time2str(date)
        sql = f'select opdep_abbrname, onlist_date, buy_stock_code, buy_stock_name ' + \
            f'from active_opdep ' + \
            f'where onlist_date = "{date}"'
        data = pd.read_sql(sql, stock)
        return data

    def active_opdep_restart_for_a_stock(date: Union[datetime.datetime, datetime.date, str], 
                                        code: str,
                                        gap: int) -> datetime:
        """
        根据给定日期和股票代码, 返回龙虎榜该股票最近一次活跃开始的时间
        date: datetime, 日期
        code: str, 股票代码
        gap: int, 定义一个重新活跃的阈值
        return: str, 返回日期
        """
        date = time2str(date)
        sql = f'select opdep_abbrname, onlist_date, buy_stock_code, buy_stock_name ' + \
            f'from active_opdep ' + \
            f'where buy_stock_code like "%%{code}%%" ' + \
            f'and onlist_date <= "{date}" ' + \
            f'order by onlist_date desc'
        data = pd.read_sql(sql, stock)
        date = data['onlist_date'].to_frame()
        date['previous'] = data['onlist_date'].shift(periods=-1)
        date['diff'] = date['onlist_date'] - date['previous']
        early_date = ''
        for i in range(len(date)):
            if date.iloc[i, :]['diff'] > datetime.timedelta(days=gap):
                early_date = date.iloc[i, :]['onlist_date'];break
        early_date = str2time(early_date)
        return early_date
    
    def active_opdep_restart_for_a_plate(date: Union[datetime.datetime, datetime.date, str],
                                        plate: str, 
                                        gap: int) -> datetime:
        """
        查询龙虎榜, 查询某一板块再给定日期下, 最近一次活跃的时间
        date: datetime, 
        plate: str, 行业
        gap: int, 定义一个重新活跃度阈值
        return
        """
        date = time2str(date)
        sql = f'select * from active_opdep_plates ' + \
            f'where plates like "%%{plate}%%" ' + \
            f'and trade_date <= "{date}"' + \
            f'order by trade_date desc ' + \
            f'limit 1000'
        data = pd.read_sql(sql, stock)
        date = data['trade_date'].to_frame()
        date['previous'] = data['trade_date'].shift(periods=-1)
        date['diff'] = date['trade_date'] - date['previous']
        early_date = ''
        for i in range(len(date)):
            if date.iloc[i, :]['diff'] > datetime.timedelta(days=gap):
                early_date = date.iloc[i, :]['trade_date'];break
        early_date = str2time(early_date)
        return early_date

    def stock2plate(date: Union[datetime.datetime, datetime.date, str],
                    code: str,
                    fields: list) -> str:
        """
        查询股票所属的板块
        date, datetime,
        code, str, 股票代码
        fields, list, 例如['swname_level3']  查询字段
        return
        """
        date = time2str(date)
        if fields is None:
            fields = '*'
        else:
            fields = ', '.join(fields)

        sql = f'select {fields} from plate_info ' + \
            f'where trade_date = "{date}" ' + \
            f'and code = "{code}"'
        data = pd.read_sql(sql, stock)
        if data.empty:
            return None
        else:
            return data.values[0][0]

    def group_stock_by_industry(date: Union[datetime.datetime, datetime.date, str],
                                plate: str, 
                                field: str) -> pd.DataFrame:
        """
        返回一个板块下的所有股票
        date: datetime, 日期
        plate: str, 行业
        field: str, 分类标准
        return: 
        """
        date = time2str(date)
        sql = f'select code ' + \
            f'from plate_info ' + \
            f'where {field} = "{plate}" ' + \
            f'and trade_date = "{date}"'
        data = pd.read_sql(sql, stock)
        return data

    def north_money_tot():
        sql = f'select date, sh_cumulative_net_buy ' + \
            f'from shhk_transaction ' + \
            f'order by date'
        data_a = pd.read_sql(sql, stock)
        sql = f'select date, sz_cumulative_net_buy ' + \
            f'from szhk_transaction ' + \
            f'order by date'
        data_b = pd.read_sql(sql, stock)
        return data_a, data_b

    def lgt_overall_holdings_api(date: Union[datetime.datetime, datetime.date, str]) -> pd.DataFrame:
        date = time2str(date)
        sql = f'select * from lgt_holdings_api ' + \
            f'where trade_date = "{date}"'
        data = pd.read_sql(sql, stock)
        return data

    def lgt_overall_holdings_em(date: Union[datetime.datetime, datetime.date, str]) -> pd.DataFrame:
        date = time2str(date)
        sql = f'select * from lgt_holdings_em ' + \
            f'where trade_date = "{date}"'
        data = pd.read_sql(sql, stock)
        return data

    def lgt_institution_holdings(date: Union[datetime.datetime, datetime.date, str],
                        inst_name: str,
                        ) -> pd.DataFrame:
        date = time2str(date)
        sql = f'select * from oversea_institution_holding ' + \
            f'where hold_date = "{date}" ' + \
            f'and org_name = "{inst_name}"'
        data = pd.read_sql(sql, stock)
        return data

    def dividend(date: Union[datetime.datetime, datetime.date, str]) -> pd.DataFrame:
        date = time2str(date)
        sql = f'select * ' + \
            f'from dividend ' + \
            f'where trade_date = "{date}"' 
        data = pd.read_sql(sql, stock)
        return data 

class StockUS():
    
    root = "https://api.stock.us/api/v1/"

    def __init__(self, sessionid: str):
        StockUS.sessionid = sessionid
        StockUS.headers = {
            "Cookie": f"sessionid={sessionid}",
            "Host": "api.stock.us",
            "Origin": "https://stock.us"
        }
    
    @classmethod
    @lru_cache(maxsize=None, typed=False)
    def index_price(cls, index: str, start: str, end: str):
        url = cls.root + f"index-price?security_code={index}&start={start}&stop={end}"
        res = Request(url, headers=cls.headers).get().json
        price = pd.DataFrame(res['price'])
        price['date'] = pd.to_datetime(price['date'])
        price = price.set_index('date')
        return price
    
    @classmethod
    @lru_cache(maxsize=None, typed=False)
    def cn_price(cls, code: str, start: str, end: str):
        url = cls.root + f"cn-price?security_code={code}&start={start}&stop={end}"
        res = Request(url, headers=cls.headers).get().json
        price = pd.DataFrame(res['price'])
        price['date'] = pd.to_datetime(price['date'])
        price = price.set_index('date')
        return price


if __name__ == '__main__':
    fetcher = StockUS("guflrppo3jct4mon7kw13fmv3dsz9kf2")
    price = fetcher.cn_price('000001.SZ', '20100101', '20200101')
    print(price)
    
    data = Databaser.lgt_overall_holdings_api('2022-01-24')
    print(data)
    # data = pd.DataFrame(np.random.rand(100, 20), 
    #     index=pd.MultiIndex.from_product([pd.date_range('20210101', periods=5), range(20)]))
    # data.fetcher.to_multisheet_excel('test.xlsx')
    

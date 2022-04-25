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
        Database.stock = create_engine(base.format(user=user, password=password, database="stock"),
            poolclass=NullPool, connect_args={"charset": "utf8", "connect_timeout": 10})
        Database.fund = create_engine(base.format(user=user, password=password, database="fund"),
            poolclass=NullPool, connect_args={"charset": "utf8", "connect_timeout": 10})
        Database.factor = create_engine(base.format(user=user, password=password, database="factor"),
            poolclass=NullPool, connect_args={"charset": "utf8", "connect_timeout": 10})
        
        today = datetime.datetime.today()
        if today.hour > 20:
            Database.today = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime(r'%Y%m%d')
        else:
            Database.today = today.strftime(r'%Y%m%d')
    
    @classmethod
    def trade_date(cls, start: str = None,
        end: str = None, freq: str = 'daily',
        weekday: int = None) -> list[str]:
        '''get trade date during a period
        ---------------------------------

        start: datetime or str
        end: datetime or date or str, end date in 3 forms
        freq: str, frequency in either 'daily', 'weekly' or 'monthly'
        '''
        if start is None:
            start = cls.today
        if end is None:
            end = cls.today
        
        query = f"select trade_date from trade_date_{freq} " \
                f"where trade_date >= '{start}' " \
                f"and trade_date <= '{end}'" 

        if weekday and freq == 'daily':
            query += f" and weekday = {weekday}"

        data = pd.read_sql(query, cls.stock)
        data = data.trade_date.sort_values().tolist()
        return data

    @classmethod
    def market_daily(cls, start: str = None,
        end: str = None,
        code: 'str | list' = None,
        fields: list = None) -> pd.DataFrame:
        '''get market data in daily frequency
        -------------------------------------

        start: datetime or date or str, start date in 3 forms
        end: datetime or date or str, end date in 3 forms
        fields: list, the field names you want to get
        '''
        # process parameters
        if start is None:
            start = "20100101"
        if end is None:
            end = cls.today

        if isinstance(code, str):
            code = [code]

        # constructing query
        if fields is None:
            fields = '*'
        else:
            fields = [f'`{i}`' for i in fields]
            fields = ', '.join(fields)

        query = f'select {fields} from market_daily' \
            f' where trade_date >= "{start}"' \
            f' and trade_date <= "{end}"' \
        
        if code:
            query += ' and '
            query += ' or '.join([f'code like "%%{c}%%"' for c in code])

        data = pd.read_sql(query, cls.stock)

        # modify dataframe index
        index = ['trade_date', 'code']
        index = list(filter(lambda x: x in fields, index))
        if index:
            data = data.set_index(index)
        return data

    @classmethod
    def index_market_daily(cls, start: str = None, end: str = None,
        code: 'str | list' = None, fields: list = None) -> pd.DataFrame:
        '''get index market data in daily frequency
        -------------------------------------

        start: datetime or date or str, start date in 3 forms
        end: datetime or date or str, end date in 3 forms
        code: str, the index code
        fields: list, the field names you want to get
        conditions: list, a series of conditions like "code = '000001.SZ'" listed in a list
        '''
        if start is None:
            start = "20100101"
        if end is None:
            end = cls.today
        if isinstance(code, str):
            code = [code]

        # constructing query
        if fields is None:
            fields = '*'
        else:
            fields = ', '.join(fields)
            
        query = f'select {fields} from index_market_daily ' \
            f'where trade_date >= "{start}"' \
            f' and trade_date <= "{end}"' \
        
        if code:
            query += ' and '
            query += ' or '.join([f'code like "%%{c}%%"' for c in code])
        
        data = pd.read_sql(query, cls.stock)

        # modify dataframe index
        index = ['trade_date', 'index_code']
        index = list(filter(lambda x: x in fields, index))
        if index:
            data = data.set_index(index)
            
        return data

    def plate_info(cls, start: 'datetime.datetime | datetime.date | str',
        end: 'datetime.datetime | datetime.date | str',
        fields: list = None, conditions: list = None) -> pd.DataFrame:
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

    def derivative_indicator(cls, start: 'datetime.datetime | datetime.date | str',
        end: 'datetime.datetime | datetime.date | str',
        fields: list = None, conditions: list = None) -> pd.DataFrame:
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
        data = pd.read_sql(sql, cls.stock)

        # modify time format
        if 'trade_dt' in fields:
            data.trade_dt = pd.to_datetime(data.trade_dt)
        
        # modify dataframe index
        index = ['trade_dt', 's_info_windcode']
        index = list(filter(lambda x: x in fields, index))
        if index:
            data = data.set_index(index)
        return data
    
    def active_opdep(cls, date: 'datetime.datetime | datetime.date | str') -> pd.DataFrame:
        """
        返回龙虎榜数据
        date: 日期
        return: 返回dataframe
        """
        date = time2str(date)
        sql = f'select opdep_abbrname, onlist_date, buy_stock_code, buy_stock_name ' + \
            f'from active_opdep ' + \
            f'where onlist_date = "{date}"'
        data = pd.read_sql(sql, cls.stock)
        return data

    def active_opdep_restart_for_a_stock(cls, 
        date: 'datetime.datetime | datetime.date | str', 
        code: str, gap: int) -> datetime:
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
        data = pd.read_sql(sql, cls.stock)
        date = data['onlist_date'].to_frame()
        date['previous'] = data['onlist_date'].shift(periods=-1)
        date['diff'] = date['onlist_date'] - date['previous']
        early_date = ''
        for i in range(len(date)):
            if date.iloc[i, :]['diff'] > datetime.timedelta(days=gap):
                early_date = date.iloc[i, :]['onlist_date'];break
        early_date = str2time(early_date)
        return early_date
    
    def active_opdep_restart_for_a_plate(date: 'datetime.datetime | datetime.date | str',
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

    def stock2plate(date: 'datetime.datetime | datetime.date | str',
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

    def group_stock_by_industry(date: 'datetime.datetime | datetime.date | str',
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

    def lgt_overall_holdings_api(date: 'datetime.datetime | datetime.date | str') -> pd.DataFrame:
        date = time2str(date)
        sql = f'select * from lgt_holdings_api ' + \
            f'where trade_date = "{date}"'
        data = pd.read_sql(sql, stock)
        return data

    def lgt_overall_holdings_em(date: 'datetime.datetime | datetime.date | str') -> pd.DataFrame:
        date = time2str(date)
        sql = f'select * from lgt_holdings_em ' + \
            f'where trade_date = "{date}"'
        data = pd.read_sql(sql, stock)
        return data

    def lgt_institution_holdings(date: 'datetime.datetime | datetime.date | str',
                        inst_name: str,
                        ) -> pd.DataFrame:
        date = time2str(date)
        sql = f'select * from oversea_institution_holding ' + \
            f'where hold_date = "{date}" ' + \
            f'and org_name = "{inst_name}"'
        data = pd.read_sql(sql, stock)
        return data

    def dividend(date: 'datetime.datetime | datetime.date | str') -> pd.DataFrame:
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
    # fetcher = StockUS("guflrppo3jct4mon7kw13fmv3dsz9kf2")
    # price = fetcher.cn_price('000001.SZ', '20100101', '20200101')
    # print(price)
    
    database = Database('kali', 'kali123')
    data = database.market_daily(code=['000001.SZ', '000002.SZ'], start='20210101', end='20211231')
    print(data)

import os
import pandas as pd
from ..tools import *
from functools import lru_cache


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

class Databaser(object):
    pass

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


if __name__ == '__main__':
    fetcher = StockUS("guflrppo3jct4mon7kw13fmv3dsz9kf2")
    price = fetcher.index_price('000001.SH', '20100101', '20200101')
    print(price)
    
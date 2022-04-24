import json
import time
import random
import requests
import numpy as np
import pandas as pd
from lxml import etree
from bs4 import BeautifulSoup


class Worker(object):
    CROSSSECTION = 1
    TIMESERIES = 2
    PANEL = 3
    
    def __init__(self, dataframe: pd.DataFrame):
        self.type_ = self._validate(dataframe)
        self.dataframe = dataframe

    def _validate(self, dataframe: pd.DataFrame):
        if not isinstance(dataframe, pd.DataFrame):
            raise TypeError('Dataframe must be a DataFrame')

        if dataframe.empty:
            raise ValueError('Dataframe is empty')

        if isinstance(dataframe.index, pd.MultiIndex):
            if isinstance(dataframe.index.levels[0], pd.DatetimeIndex) and len(dataframe.index.levshape) == 2:
                return Worker.PANEL
            
            else:
                raise TypeError('A panel index must be 2 level index with DatetimeIndex as level 0')
        
        else:
            if not isinstance(dataframe.index, pd.DatetimeIndex):
                return Worker.CROSSSECTION
            else:
                return Worker.TIMESERIES

    def _indexer(self, datetime, asset, indicator):
        if self.type_ == Worker.CROSSSECTION:
            return self.dataframe.loc[(asset, indicator)].copy()

        elif self.type_ == Worker.TIMESERIES:
            return self.dataframe.loc[(datetime, indicator)].copy()
            
        elif self.type_ == Worker.PANEL:
            if isinstance(datetime, str):
                return self.dataframe.loc[(datetime, asset), indicator].droplevel(0).copy()
            else:
                if isinstance(asset, str):
                    return self.dataframe.loc[(datetime, asset), indicator].droplevel(1).copy()
                if isinstance(indicator, str):
                    return self.dataframe.loc[(datetime, asset), indicator].unstack(level=1).copy()
                return self.dataframe.loc[(datetime, asset), indicator].copy()

class Request(object):

    def __init__(self, url, headers: dict = None, **kwargs):
        self.url = url
        if headers:
            headers.update(self.header())
            self.headers = headers
        self.kwargs = kwargs
        
    def header(self):
        ua_list = [
            'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/30.0.1599.101',
            'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.122',
            'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71',
            'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95',
            'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71',
            'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E)',
            'Mozilla/5.0 (Windows NT 5.1; U; en; rv:1.8.1) Gecko/20061208 Firefox/2.0.0 Opera 9.50',
            'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:34.0) Gecko/20100101 Firefox/34.0',
            ]
        base_header = {
            "User-Agent": random.choice(ua_list),
            'Accept': '*/*',
            'Connection': 'keep-alive',
            'Accept-Language': 'zh-CN,zh;q=0.8'
        }
        return base_header

    def get(self):
        try:
            response = requests.get(self.url, headers=self.headers, **self.kwargs)
            response.raise_for_status()        
            self.response = response
            print(f'[+] {self.url} Get Success!')
            return self
        except Exception as e:
            print(f'[-] Error: {e}')

    def post(self):
        try:
            response = requests.post(self.url, headers=self.headers, **self.kwargs)
            response.raise_for_status()        
            self.response = response
            print(f'[+] {self.url} Post Success!')
            return self
        except Exception as e:
            print(f'[-] Error: {e}')

    @property
    def etree(self):
        return etree.HTML(self.response.text)

    @property
    def json(self):
        return json.loads(self.response.text)
    
    @property
    def soup(self):
        return BeautifulSoup(self.response.text, 'lxml')

class ProxyRequest(Request):
    
    def __init__(self, url, headers: dict = None, 
        proxies: dict = None, timeout: int = None, 
        retry: int = None, retry_delay: float = None,**kwargs):
        super().__init__(url, headers, **kwargs)
        self.proxies = {} if proxies is None else proxies
        self.timeout = 2 if timeout is None else timeout
        self.retry = -1 if retry is None else retry
        self.retry_delay = 0 if retry_delay is None else retry_delay
        self.kwargs = kwargs
    
    def get(self):
        if isinstance(self.proxies, dict):
            self.proxies = [self.proxies]
        random.shuffle(self.proxies) 
        if self.retry == -1:
            self.retry = len(self.proxies)
        for try_times, proxy in enumerate(self.proxies):
            if try_times + 1 <= self.retry:
                try:
                    response = requests.get(self.url, headers=self.headers, proxies=proxy, **self.kwargs)
                    response.raise_for_status()
                    self.response = response
                    print(f'[+] {self.url}, try {try_times + 1}/{self.retry}')
                    return self
                except Exception as e:
                    print(f'[-] [{e}] {self.url}, try {try_times + 1}/{self.retry}')
                    time.sleep(self.retry_delay)

    def post(self):
        if isinstance(self.proxies, dict):
            self.proxies = [self.proxies]
        random.shuffle(self.proxies) 
        if self.retry == -1:
            self.retry = len(self.proxies)
        for try_times, proxy in enumerate(self.proxies):
            if try_times + 1 <= self.retry:
                try:
                    response = requests.post(self.url, headers=self.headers, proxies=proxy, **self.kwargs)
                    response.raise_for_status()
                    self.response = response
                    print(f'[+] {self.url}, try {try_times + 1}/{self.retry}')
                    return self
                except Exception as e:
                    print(f'[-] [{e}] {self.url}, try {try_times + 1}/{self.retry}')
                    time.sleep(self.retry_delay)

    def get_async(self, container: dict):
        if isinstance(self.proxies, dict):
            self.proxies = [self.proxies]
        random.shuffle(self.proxies) 
        if self.retry == -1:
            self.retry = len(self.proxies)
        for try_times, proxy in enumerate(self.proxies):
            if try_times + 1 <= self.retry:
                try:
                    response = requests.get(self.url, headers=self.headers, proxies=proxy, **self.kwargs)
                    response.raise_for_status()
                    self.response = response
                    container[self.url] = self.process()
                    print(f'[+] {self.url}, try {try_times + 1}/{self.retry}')
                    break
                except Exception as e:
                    print(f'[-] [{e}] {self.url}, try {try_times + 1}/{self.retry}')
                    time.sleep(self.retry_delay)

    def post_async(self, container: dict):
        if isinstance(self.proxies, dict):
            self.proxies = [self.proxies]
        random.shuffle(self.proxies) 
        if self.retry == -1:
            self.retry = len(self.proxies)
        for try_times, proxy in enumerate(self.proxies):
            if try_times + 1 <= self.retry:
                try:
                    response = requests.post(self.url, headers=self.headers, proxies=proxy, **self.kwargs)
                    response.raise_for_status()
                    self.response = response
                    container[self.url] = self.process()
                    print(f'[+] {self.url}, try {try_times + 1}/{self.retry}')
                    break
                except Exception as e:
                    print(f'[-] [{e}] {self.url}, try {try_times + 1}/{self.retry}')
                    time.sleep(self.retry_delay)

    def process(self):
        raise NotImplementedError

if __name__ == "__main__":
    pass
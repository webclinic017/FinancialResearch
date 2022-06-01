import pandas as pd


def ma(data: pd.DataFrame, period: int):
    return data['close'].rolling(int(period)).mean()

def bolltop(data: pd.DataFrame, period: int, n: int):
    period = int(period)
    n = int(n)
    bollmid = data['close'].rolling(period).mean()
    dev = data['close'].rolling(period).std()
    top = bollmid + n * dev
    return top

def bollbot(data: pd.DataFrame, period: int, n: int):
    period = int(period)
    n = int(n)
    bollmid = data['close'].rolling(period).mean()
    dev = data['close'].rolling(period).std()
    bot = bollmid - n * dev
    return bot

def atr(data: pd.DataFrame, period: int):
    return (data['high'] / data['low'] - 1).rolling(int(period)).mean()

__all__ = [
    'ma',
    'bolltop',
    'bollbot',
    'atr',
]

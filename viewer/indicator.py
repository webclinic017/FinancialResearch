import pandas as pd


def ma(data: pd.DataFrame, period: int):
    return data['close'].rolling(int(period)).mean()

def bolling(data: pd.DataFrame, period: int, n: int):
    bollmid = data['close'].rolling(int(period)).mean()
    dev = data['close'].rolling(int(period)).std()
    bolltop = bollmid + n * dev
    bollbot = bollmid - n * dev
    return pd.concat([bollmid, bolltop, bollbot], axis=1)

def atr(data: pd.DataFrame, period: int):
    return (data['high'] / data['low'] - 1).rolling(int(period)).mean()

__all__ = [
    'ma',
    'bolling',
    'atr',
]

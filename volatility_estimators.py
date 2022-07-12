import pandas as pd
import numpy as np

def rogers_satchell_vol(ohlc_data, time_step=1):
    op = ohlc_data['open'].values
    lo = ohlc_data['low'].values
    hi = ohlc_data['high'].values
    cl = ohlc_data['close'].values
    argument = np.log(hi/cl) * np.log(hi/op) + np.log(lo/cl) * np.log(lo/op)
    return np.sqrt(1 / op.shape[0] * np.sum(argument))

def garman_klass_vol(ohlc_data, time_step=1):
    op = ohlc_data['open'].values
    lo = ohlc_data['low'].values
    hi = ohlc_data['high'].values
    cl = ohlc_data['close'].values
    argument = 1/2 * np.log(hi/lo) ** 2 - (2 * np.log(2) - 1) * np.log(cl/op) ** 2 
    return np.sqrt(1 / op.shape[0] * np.sum(argument))

def close_to_close_vol(close_data):
    time, price = (data['tick'].to_numpy(), data['close'].to_numpy())
    x = np.log(price[1:]) - np.log(price[:-1])
    mu = x.mean()
    vol = np.std(x-mu, ddof=1)
    return mu, vol

def c2c_vol(ohlc, time_step=1):
    return np.log(1+ohlc.close.pct_change()).std()

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

def return_vol_estimator(close_data,  time_step=1):
    price = pd.Series(close_data).values
    x = np.log(price[1:]) - np.log(price[:-1])
    xavg = x.mean()
    xvar = x.var(ddof=1)
    mu = xavg / time_step + xvar / (2 * time_step)
    vol = np.sqrt(xvar / time_step)
    return mu, vol

def c2c_vol(ohlc, time_step=1):
    return np.log(1+ohlc.close.pct_change()).std()

def return_vol_error(close_data, time_step=1):
    mu, vol = return_vol_estimator(close_data)    
    mu_error = np.sqrt(vol ** 4 / 2 + vol ** 2 / time_step) / np.sqrt(close_data.shape[0])
    vol_error = np.sqrt(vol ** 2 / 2) / np.sqrt(close_data.shape[0])
    return mu_error, vol_error

def z_score_trend_indicator(close_data, time_step=1):
    mu, vol = return_vol_estimator(close_data)
    mu_error, vol_error = return_vol_error(close_data, time_step=1)
    zscore = mu / mu_error
    return mu, zscore
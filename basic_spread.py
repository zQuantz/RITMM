import numpy as np
import pandas as pd

ohlc = pd.read_csv('data/ohlc.csv')
tas = pd.read_csv('data/tas.csv')
tasagg = pd.read_csv('data/tasagg.csv')


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

def return_vol_estimator(close_data):
    time, price = (data['tick'].to_numpy(), data['close'].to_numpy())
    x = np.log(price[1:]) - np.log(price[:-1])
    mu = x.mean()
    vol = np.std(x-mu, ddof=1)
    return mu, vol

def perf_bid_ask_factor(ohlc, tasagg, calibration):
    time_factor = ((tassagg['avg_trade_volume']/ (tassagg['total_volume'] + 1)) ** 2).values.mean()
    vol = rogers_satchell_vol(ohlc, time_step=1)
    spread = calibration * time_factor * vol
    return spread

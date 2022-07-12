from volatility_estimators import *
from config import API_KEY
from pathlib import Path
from time import sleep
import pandas as pd
import numpy as np
import requests
import sys, os
import signal

###################################################################################################

DIR = Path(os.path.dirname(os.path.realpath(__file__)))

shutdown = False

MAX_VOLUME = 5_000
MAX_POSITION = 25_000
MKT_COM = 0.01
LMT_COM = 0.005

BOOK_LIMIT = 40
VOL_CALIBRATION = 2

data = {
    'tick': 0,
    'position': 0,
    'bid_vwap': 0,
    'ask_vwap': 0,
    'vol': 0,
    'vol_spread': 0,
    'bid_ladder': pd.DataFrame(),
    'best_bid': 0,
    'best_ask': 0,
    'best_spread': 0,
    'mid': 0,
    'ask_ladder': pd.DataFrame(),
    'ohlc': pd.DataFrame(),
    'bid_price': 0,
    'ask_price': 0,
    'n_transacted_orders': 0
}

###################################################################################################

class ApiException(Exception):
    pass

def signal_handler(signum, frame):
    global shutdown
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    shutdown = True

def get_tick(session, data):
    resp = session.get("http://localhost:9999/v1/case")
    if resp.ok:
        data['tick'] = resp.json()['tick']
        return data
    return ApiException("Auth Error. Check API Key")

def get_security_info(session, data):
    resp = session.get("http://localhost:9999/v1/securities?ticker=ALGO")
    if resp.ok:
        resp = resp.json()
        data['position'] = resp[0]['position']
        return data
    return ApiException("Auth Error. Check API Key")

OBKEYS = ['price', 'quantity', 'quantity_filled']
def aggregate_order_book(items, ascending):
    df = pd.DataFrame(
        [
            {
                key: bid[key]
                for key in OBKEYS
            }
            for bid in items
            if bid['status'] == "OPEN"
        ],
        columns = OBKEYS
    )
    df['quantity'] = df.quantity - df.quantity_filled
    df = df[df.quantity != 0].drop('quantity_filled', axis=1)
    df = df.groupby('price').sum().sort_index(ascending = ascending).reset_index()
    vwap = (df.price * (df.quantity / df.quantity.sum())).sum()
    return df, vwap

def get_order_book(session, data):
    resp = session.get(f"http://localhost:9999/v1/securities/book?ticker=ALGO&limit={BOOK_LIMIT}")
    if resp.ok:
        resp = resp.json()
        data['bid_ladder'], data['bid_vwap'] = aggregate_order_book(resp['bids'], False)
        data['ask_ladder'], data['ask_vwap'] = aggregate_order_book(resp['asks'], False)
        if data['bid_ladder'].shape[0] == 0 or data['ask_ladder'].shape[0] == 0:
            return data
        data['best_bid'] = data['bid_ladder'].price.values[0]
        data['best_ask'] = data['ask_ladder'].price.values[-1]
        data['mid'] = round((data['best_bid'] + data['best_ask']) / 2, 3)
        data['best_spread'] = round(data['best_ask'] - data['best_bid'], 2)
        return data
    return ApiException("Auth Error. Check API Key")

HISTCOLS = ['tick', 'open', 'high', 'low', 'close']
def get_price_history(session, data):
    resp = session.get(f"http://localhost:9999/v1/securities/history?ticker=ALGO")
    if resp.ok:
        df = pd.DataFrame(resp.json(), columns = HISTCOLS).sort_values('tick', ascending = True)
        vol = np.log(1 + df.close.pct_change()).dropna()
        data['ohlc'] = df
        return data
    return ApiException("Auth Error. Check API Key")

TASCOLS = ['id', 'period', 'tick', 'price', 'quantity']
def get_time_and_sales(session, data):
    resp = session.get(f"http://localhost:9999/v1/securities/tas?ticker=ALGO")
    if resp.ok:
        df = pd.DataFrame(resp.json(), columns = TASCOLS).sort_values('tick', ascending = True)
        df = df.groupby('tick').agg({'quantity': ['mean', 'sum']}).reset_index()
        df.columns = ['tick', 'avg_trade_volume', 'total_volume']
        data['tas'] = df
    return ApiException("Auth Error. Check API Key")

def get_open_orders(session):
    resp = session.get(f"http://localhost:9999/v1/orders?status=OPEN")
    if resp.ok: return resp.json()
    return ApiException("Auth Error. Check API Key")

def get_transacted_orders(session):
    resp = session.get(f"http://localhost:9999/v1/orders?status=TRANSACTED")
    if resp.ok: return resp.json()
    return ApiException("Auth Error. Check API Key")

def get_order_details(session, _id):
    resp = session.get(f"http://localhost:9999/v1/orders?{_id}")
    if resp.ok: return resp.json()
    return ApiException("Auth Error. Check API Key")

## Subject to rate limits
def send_order(session, data, _type, quantity, action, price = 0):
    params = {
        'ticker': 'ALGO',
        'type': _type,
        'quantity': quantity,
        'price': str(price).replace(".", ","),
        'action': action,
    }
    if _type == "MARKET": body['dry_run'] = 0
    resp = session.post("http://localhost:9999/v1/orders", params = params)
    if resp.status_code == 429:
        sleep(resp.json()['wait'] / 1_000)
        resp = session.post("http://localhost:9999/v1/orders", params = params)
    if resp.ok:
        return resp.json()['order_id']
    return ApiException("Auth Error. Check API Key")

def cancel_all_orders(session):
    resp = session.post(f"http://localhost:9999/v1/commands/cancel?all=1")
    if resp.ok:
        print("Cacelled All Orders")
    return ApiException("Auth Error. Check API Key")

def bookkeeping(orders):
    buy_orders = [
        order
        for order in orders
        if order['action'] == "BUY"
    ]
    sell_orders = [
        order
        for order in orders
        if order['action'] == "SELL"
    ]

def vol_spreading(data, estimator, calibration):
    ohlc = data['ohlc']
    tassagg = data['tas']
    data['time_factor'] = ((tassagg['avg_trade_volume'] / (tassagg['total_volume'] + 1)) ** 2).values.mean()
    data['vol'] = estimator(ohlc, time_step=1)
    data['vol_spread'] = round(calibration * data['time_factor'] * data['vol'] * data['mid'], 2)

def inventory_skewing(data):

    pos = data['position']
    F = abs(pos // MAX_VOLUME) * 2

    if pos > 0:
        ask = data['mid'] + data['vol_spread'] * (1 / (1 + F))
        bid = data['mid'] - (1 + F) * data['vol_spread']
    elif pos < 0:
        ask = data['mid'] + (1 + F) * data['vol_spread']
        bid = data['mid'] - data['vol_spread'] * (1 / (1 + F))
    else:
        ask = data['mid'] + data['vol_spread']
        bid = data['mid'] - data['vol_spread']

    return round(bid, 2), round(ask, 2)

###################################################################################################

def main():

    tick = 0
    position = 0
    init = True

    with requests.Session() as session:
        session.headers.update(API_KEY)

        while data['tick'] != 299:
            
            ## Dont trade for 5 seconds
            get_tick(session, data)
            if data['tick'] < 5:
                continue

            transacted_orders = get_transacted_orders(session)
            n = data['n_transacted_orders']
            if len(transacted_orders) != n or init:
                
                cancel_all_orders(session)

                get_security_info(session, data)
                get_order_book(session, data)
                get_price_history(session, data)
                get_time_and_sales(session, data)
                
                vol_spreading(data, c2c_vol, VOL_CALIBRATION)
                bid, ask = inventory_skewing(data)
                
                print("------------")
                print("Tick", data['tick'])
                print("N-transaction", len(transacted_orders))
                print("Bid", bid)
                print("Mid", data['mid'])
                print("Ask", ask)
                print("Position", data['position'])
                print("Volatility", round(data['vol'] * 100, 4))
                print("Vol-Spread", data['vol_spread'])
                print("Time-Factor", data['time_factor'])
                print("Abs-Spread", round(ask - bid, 2))

                send_order(session, data, "LIMIT", MAX_VOLUME, "BUY", bid)
                send_order(session, data, "LIMIT", MAX_VOLUME, "SELL", ask)
                
                data['n_transacted_orders'] = len(transacted_orders)
                init = False

if __name__ == '__main__':

    signal.signal(signal.SIGINT, signal_handler)
    main()
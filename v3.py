from volatility_estimators import *
from logger import DIR, logger
from config import API_KEY
from time import sleep
import pandas as pd
import numpy as np
import requests
import sys, os
import signal
import locale

###################################################################################################

LOC = locale.getdefaultlocale()[0]
shutdown = False

MAX_VOLUME = 5_000
MAX_POSITION = 25_000
MKT_COM = 0.01
LMT_COM = 0.005

BOOK_LIMIT = 40
N_ORDERS = 1
ROLLING_TREND_LOOKBACK = 50

VOL_CALIBRATION = 0.75
TREND_SKEW_CALIBRATION = 1
INVENTORY_SKEW_CALIBRATION = 3
STOP_LOSS_CALIBRATION = 1

START_TICK = 10
END_TICK = 290

data = {
    ## Case Info
    'tick': 0,

    ## Security Info
    'position': 0,
    'position_vwap': 0,
    'last': 0,
    'bid': 0,
    'mid': 0,
    'ask': 0,
    'realized_profit': 0,

    ## Order Book Info
    'bid_ladder': pd.DataFrame(),
    'ask_ladder': pd.DataFrame(),
    'bid_vwap': 0,
    'ask_vwap': 0,
    'LOB_imbalance': 0,
    'LOB_mass_imbalance': 0,

    ## Price History
    'ohlc': pd.DataFrame(),

    ## Time & Sales
    'tas': pd.DataFrame(),
    
    ## Calculated Metrics
    'gtrend': 0,
    'trend': 0,
    'gtrend_confidence': 0,
    'trend_confidence': 0,
    'vol': 0,
    'vol_spread': 0,
    'time_factor': 0,
    'current_bid': 0,
    'current_ask': 0,
    'n_open_orders': 0,
    'pnl': 0,
    'pnl_threshold': 0
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
        resp = resp.json()[0]
        data['position'] = resp['position']
        data['position_vwap'] = resp['vwap']
        data['last'] = resp['last']
        data['mid'] = round((resp['bid'] + resp['ask']) / 2, 2)
        data['bid'] = resp['bid']
        data['ask'] = resp['ask']
        data['realized_profit'] = resp['realized']
        return data
    return ApiException("Auth Error. Check API Key")

HISTCOLS = ['tick', 'open', 'high', 'low', 'close']
def get_price_history(session, data):
    resp = session.get(f"http://localhost:9999/v1/securities/history?ticker=ALGO")
    if resp.ok:
        
        data['ohlc'] = pd.DataFrame(resp.json(), columns = HISTCOLS).sort_values('tick', ascending = True)
        
        data['vol'] = c2c_vol(data['ohlc'], time_step=1)
        data['gtrend'], data['gtrend_confidence'] = z_score_trend_indicator(data['ohlc'].close)

        df = data['ohlc'].iloc[-ROLLING_TREND_LOOKBACK:].copy()
        data['trend'], data['trend_confidence'] = z_score_trend_indicator(df.close)

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

def build_order(_type, quantity, price, action):
    return {
        'ticker': 'ALGO',
        'type': _type,
        'quantity': quantity,
        'price': price,
        'action': action,
    }

def get_orders(data, bid, ask):

    pos = data['position']
    n = abs(int(pos // MAX_VOLUME))
    r = abs(int(pos % MAX_VOLUME))
    if pos > 0:
        buy_orders = [
            build_order("LIMIT", MAX_VOLUME, bid, "BUY")
            for i in range(N_ORDERS)
        ]
        sell_orders = [
            build_order("LIMIT", MAX_VOLUME, ask, "SELL")
            for i in range(n)
        ]
        sell_orders.append(build_order("LIMIT", r, ask, "SELL"))
    elif pos < 0:
        sell_orders = [
            build_order("LIMIT", MAX_VOLUME, ask, "SELL")
            for i in range(N_ORDERS)
        ]
        buy_orders = [
            build_order("LIMIT", MAX_VOLUME, ask, "BUY")
            for i in range(n)
        ]
        buy_orders.append(build_order("LIMIT", r, ask, "BUY"))
    else:
        buy_orders = [
            build_order("LIMIT", MAX_VOLUME, bid, "BUY")
            for i in range(N_ORDERS)
        ]
        sell_orders = sell_orders = [
            build_order("LIMIT", MAX_VOLUME, ask, "SELL")
            for i in range(N_ORDERS)
        ]

    buy_orders = [order for order in buy_orders if order['quantity'] != 0][::-1]
    sell_orders = [order for order in sell_orders if order['quantity'] != 0][::-1]
    return buy_orders, sell_orders

def get_liquidating_orders(data, isMarket = False):

    pos = data['position']
    n = abs(int(pos // MAX_VOLUME))
    r = abs(int(pos % MAX_VOLUME))
    action = "BUY" if pos < 0 else "SELL"
    price = data['bid'] if pos < 0 else data['ask']

    orders = [
        build_order("MARKET" if isMarket else "LIMIT", MAX_VOLUME, price, action)
        for i in range(n)
    ]
    orders.append(build_order("MARKET" if isMarket else "LIMIT", r, price, action))
    return [order for order in orders if order['quantity'] != 0]

## Subject to rate limits
def send_order(session, order):
    if order['type'] == "MARKET": order['dry_run'] = 0
    resp = session.post("http://localhost:9999/v1/orders", params = order)
    if resp.status_code == 429:
        sleep(resp.json()['wait'] / 1_000)
        resp = session.post("http://localhost:9999/v1/orders", params = order)
    if resp.ok:
        return resp.json()['order_id']
    return ApiException("Auth Error. Check API Key")

def cancel_all_orders(session):
    resp = session.post(f"http://localhost:9999/v1/commands/cancel?all=1")
    if resp.ok:
        print("Cacelled All Orders")
    return ApiException("Auth Error. Check API Key")

def vol_spreading(data, calibration):
    ohlc = data['ohlc']
    tas = data['tas']
    data['time_factor'] = ((tas['avg_trade_volume'] / (tas['total_volume'] + 1)) ** 2).values.mean()
    data['vol_spread'] = round(calibration * data['vol'] * data['mid'], 2)

    # data['vol_spread'] = round(calibration * data['time_factor'] * data['vol'] * data['mid'], 2)

def trend_skewing(data):
    ask = data['mid'] + data['vol_spread']
    bid = data['mid'] - data['vol_spread']

    if data['gtrend_confidence'] > 1.5:
    	trend = data['gtrend']
    	vol = data['vol']
    	
    	ask = np.exp(TREND_SKEW_CALIBRATION * trend / np.sqrt(vol)) * ask
    	bid = np.exp(TREND_SKEW_CALIBRATION * trend / np.sqrt(vol)) * bid

    	if bid >= data['mid']:
    		bid = data['mid']

    	if ask <= data['mid']:
    		ask = data['mid']

    return round(bid, 2), round(ask, 2)

def inventory_skewing(data, bid, ask):

    pos = data['position']
    F = abs(pos // MAX_VOLUME) * INVENTORY_SKEW_CALIBRATION

    if pos > 0:
        # ask = data['mid'] + (ask - data['mid']) * (1 / (1 + F))
        bid = data['mid'] - (1 + F) * (data['mid'] - bid)
    elif pos < 0:
        ask = data['mid'] + (1 + F) * (ask - data['mid'])
        # bid = data['mid'] - (data['mid'] - bid) * (1 / (1 + F))

    return round(bid, 2), round(ask, 2)

###################################################################################################

## Add something to monitor trades while we are in them rather than only updating the information
## once we get another execution.

def main():

    tick = 0
    ticks_since_cancel = 0
    bid_partial_sent = False
    ask_partial_sent = False

    with requests.Session() as session:
        session.headers.update(API_KEY)

        while data['tick'] != 299 and not shutdown:
            
            ## Dont trade for first n ticks
            get_tick(session, data)
            if data['tick'] < START_TICK:
                continue

            ## Liquidate everything with 10 ticks remaining
            if data['tick'] > END_TICK:
                # get_order_book(session, data)
                cancel_all_orders(session)
                orders = get_liquidating_orders(data)
                for order in orders:
                    send_order(session, order)

            get_security_info(session, data)
            get_price_history(session, data)
            get_time_and_sales(session, data)

            if data['tick'] != tick:
                
                if data['position'] == 0 or data['tick'] - ticks_since_cancel > 5:
                    cancel_all_orders(session)
                    ticks_since_cancel = data['tick']
                    bid_partial_sent = False
                    ask_partial_sent = False

                vol_spreading(data, VOL_CALIBRATION)
                bid, ask = trend_skewing(data)
                bid, ask = inventory_skewing(data, bid, ask)
                data['current_bid'] = bid
                data['current_ask'] = ask

                pos = data['position']
                r = pos % MAX_VOLUME
                bid_order = build_order("LIMIT", MAX_VOLUME, bid, "BUY")
                send_order(session, bid_order)
                if r > 0 and not bid_partial_sent:
                    bid_order = build_order("LIMIT", r, bid, "BUY")
                    send_order(session, bid_order)
                    bid_partial_sent = True

                ask_order = build_order("LIMIT", MAX_VOLUME, ask, "SELL")
                send_order(session, ask_order)
                if r < 0 and not ask_partial_sent:
                    bid_order = build_order("LIMIT", abs(r), ask, "SELL")
                    send_order(session, bid_order)
                    ask_partial_sent = True

                print("------------")
                print("Tick", data['tick'])
                print("Tick Since Last Cancel", data['tick'] - ticks_since_cancel)
                print("Position", data['position'])
                print(f"({bid}, {data['mid']}, {ask})")
                print("Volatility", round(data['vol'] * 100, 4))
                print("Vol-Spread", data['vol_spread'])

                tick = data['tick']

if __name__ == '__main__':

    signal.signal(signal.SIGINT, signal_handler)
    main()
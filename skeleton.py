from volatility_estimators import *
from logger import DIR, logger
from config import API_KEY
from time import sleep
import pandas as pd
import numpy as np
import requests
import sys, os
import signal

###################################################################################################

shutdown = False

MAX_VOLUME = 5_000
MAX_POSITION = 25_000
MKT_COM = 0.01
LMT_COM = 0.005

BOOK_LIMIT = 40
VOL_CALIBRATION = 2.5
N_ORDERS = 1
ROLLING_TREND_LOOKBACK = 50
TREND_SKEW_CALIBRATION = 1

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
    'pmid': 0,
    'ask': 0,

    ## Order Book Info
    'best_bid': 0,
    'best_ask': 0,
    'mid': 0,
    'best_spread': 0,
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
        resp = resp.json()[0]
        data['position'] = resp['position']
        data['position_vwap'] = resp['vwap'] 
        data['last'] = resp['last']
        data['pmid'] = round((resp['bid'] + resp['ask']) / 2, 2)
        data['bid'] = resp['bid']
        data['ask'] = resp['ask']
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
        
        ## Order Book Statistics
        data['best_bid'] = data['bid_ladder'].price.values[0]
        data['best_ask'] = data['ask_ladder'].price.values[-1]
        data['mid'] = round((data['best_bid'] + data['best_ask']) / 2, 3)
        data['best_spread'] = round(data['best_ask'] - data['best_bid'], 2)
        
        bid_volume = data['bid_ladder'].quantity.sum()
        ask_volume = data['ask_ladder'].quantity.sum()
        data['LOB_imbalance'] = (bid_volume - ask_volume) / (bid_volume + ask_volume)

        bid_vwap_spread = data['mid'] - data['bid_vwap']
        ask_vwap_spread = data['ask_vwap'] - data['mid']
        data['LOB_mass_imbalance'] =  (ask_vwap_spread - bid_vwap_spread) / (bid_vwap_spread + ask_vwap_spread)
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

def get_liquidating_orders(data):

    pos = data['position']
    n = abs(int(pos // MAX_VOLUME))
    r = abs(int(pos % MAX_VOLUME))
    action = "BUY" if pos < 0 else "SELL"
    price = data['best_bid'] if pos < 0 else data['best_ask']

    orders = [
        build_order("LIMIT", MAX_VOLUME, price, action)
        for i in range(n)
    ]
    orders.append(build_order("LIMIT", r, price, action))
    return [order for order in orders if order['quantity'] != 0]

## Subject to rate limits
def send_order(session, order):
    if order['type'] == "MARKET": body['dry_run'] = 0
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
    tassagg = data['tas']
    data['time_factor'] = ((tassagg['avg_trade_volume'] / (tassagg['total_volume'] + 1)) ** 2).values.mean()
    data['vol_spread'] = round(calibration * data['time_factor'] * data['vol'] * data['mid'], 2)

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
    F = abs(pos // MAX_VOLUME)

    if pos > 0:
        ask = data['mid'] + (ask - data['mid']) * (1 / (1 + F))
        bid = data['mid'] - (1 + F) * (data['mid'] - bid)
    elif pos < 0:
        ask = data['mid'] + (1 + F) * (ask - data['mid'])
        bid = data['mid'] - (data['mid'] - bid) * (1 / (1 + F))

    return round(bid, 2), round(ask, 2)

def init_log():
    logger.info("tick,last,bid,pmid,ask,best_bid,mid,best_ask,bid_vwap,ask_vwap,LOB_imbalance,LOB_mass_imbalance,vol,time_factor,vol_spread,trend,trend_confidence,gtrend,gtrend_confidence,position,current_bid,position_vwap,current_ask")

def log(data):
    logger.info(f"{data['tick']},{data['last']},{data['bid']},{data['pmid']},{data['ask']},{data['best_bid']},{data['mid']},{data['best_ask']},{data['bid_vwap']},{data['ask_vwap']},{data['LOB_imbalance']},{data['LOB_mass_imbalance']},{data['vol']},{data['time_factor']},{data['vol_spread']},{data['trend']},{data['trend_confidence']},{data['gtrend']},{data['gtrend_confidence']},{data['position']},{data['current_bid']},{data['position_vwap']},{data['current_ask']}")

###################################################################################################

## Add something to monitor trades while we are in them rather than only updating the information
## once we get another execution.

def main():

    tick = 0
    position = 0
    init = True

    init_log()

    with requests.Session() as session:
        session.headers.update(API_KEY)

        while data['tick'] != 299:
            
            ## Dont trade for first 5 ticks
            get_tick(session, data)
            if data['tick'] < START_TICK:
                continue

            ## Liquidate everything with 10 ticks remaining
            if data['tick'] > END_TICK:
                get_order_book(session, data)
                cancel_all_orders(session)
                orders = get_liquidating_orders(data)
                for order in orders:
                    send_order(session, order)

            get_security_info(session, data)
            get_order_book(session, data)
            get_price_history(session, data)
            get_time_and_sales(session, data)

            log(data)

            transacted_orders = get_transacted_orders(session)
            n = data['n_transacted_orders']
            if len(transacted_orders) != n or init:
                
                cancel_all_orders(session)
                
                vol_spreading(data, VOL_CALIBRATION)
                bid, ask = trend_skewing(data)
                bid, ask = inventory_skewing(data)
                data['current_bid'] = bid
                data['current_ask'] = ask
                
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
                print("Best BBO Spread", round(data['best_spread'], 2))
                print("LOB Imbalance", round(100 * data['LOB_imbalance'], 2))
                print("LOB Mass Imbalance", round(100 * data['LOB_mass_imbalance'], 2))

                buy_orders, sell_orders = get_orders(data, bid, ask)
                for order in buy_orders:
                    send_order(session, order)
                for order in sell_orders:
                    send_order(session, order)
                
                data['n_transacted_orders'] = len(transacted_orders)
                init = False

if __name__ == '__main__':

    signal.signal(signal.SIGINT, signal_handler)
    main()
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
MAX_ORDERS = 3

N_ORDERS = 1
ROLLING_TREND_LOOKBACK = 10

VOL_CALIBRATION = 1
TREND_SKEW_CALIBRATION = 3
TIGHT_INVENTORY_SKEW_CALIBRATION = 0.5
LOOSE_INVENTORY_SKEW_CALIBRATION = 2
ORDER_PROXIMITY_CALIBRATION = 0.5

OLD_ORDER_CALIBRATION = 7
MAX_HOLDING_PERIOD = 20
MAX_HOLDING_CALIBRATION = 2

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

	## Calculated Metrics
	'gtrend': 0,
	'trend': 0,
	'gtrend_confidence': 0,
	'trend_confidence': 0,
	'trend_factor': 0,
	'trend_factor_mmm': 0,
	'vol': 0,
	'vol_spread': 0,
	'current_bid': 0,
	'current_ask': 0,

	## Order Stuff
	'bid_orders': [],
	'ask_orders': []
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
		return data
	return ApiException("Auth Error. Check API Key")

HISTCOLS = ['tick', 'open', 'high', 'low', 'close']
def get_price_history(session, data):
	resp = session.get(f"http://localhost:9999/v1/securities/history?ticker=ALGO")
	if resp.ok:
		
		ohlc = pd.DataFrame(resp.json(), columns = HISTCOLS).sort_values('tick', ascending = True)
		
		data['vol'] = c2c_vol(ohlc, time_step=1)
		data['gtrend'], data['gtrend_confidence'] = z_score_trend_indicator(ohlc.close)

		df = ohlc.iloc[-ROLLING_TREND_LOOKBACK:].copy()
		data['trend'], data['trend_confidence'] = z_score_trend_indicator(df.close)

		return data
	return ApiException("Auth Error. Check API Key")

## Order Functions
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

TASCOLS = ['id', 'period', 'tick', 'price', 'quantity']
def get_time_and_sales(session, data):
    resp = session.get(f"http://localhost:9999/v1/securities/tas?ticker=ALGO")
    if resp.ok:
        df = pd.DataFrame(resp.json(), columns = TASCOLS).sort_values('tick', ascending = True)
        df = df.groupby('tick').agg({'quantity': ['mean', 'sum']}).reset_index()
        df.columns = ['tick', 'avg_trade_volume', 'total_volume']
        data['time_factor'] = ((df['avg_trade_volume'] / (df['total_volume'] + 1)) ** 2).values.mean()
    return ApiException("Auth Error. Check API Key")

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
	r = abs(int(pos % MAX_VOLUME))
	if pos > 0:
		buy_orders = [
			build_order("LIMIT", MAX_VOLUME, bid, "BUY")
			for i in range(N_ORDERS)
		]
		sell_orders = [
			build_order("LIMIT", MAX_VOLUME, ask, "SELL")
			for i in range(N_ORDERS)
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

def execute_orders(session, data, bid, ask, open_bids, open_asks):

	bid_order = build_order("LIMIT", MAX_VOLUME, bid, "BUY")
	bid_oid = send_order(session, bid_order)
	data['bid_orders'].append((bid_oid, bid, data['tick'], MAX_VOLUME))

	ask_order = build_order("LIMIT", MAX_VOLUME, ask, "SELL")
	ask_oid = send_order(session, ask_order)
	data['ask_orders'].append((ask_oid, ask, data['tick'], MAX_VOLUME))

	r = abs(data['position']) % MAX_VOLUME
	if r == 0:
		orders = open_bids if data['position'] > 0 else open_asks
		for order in orders:
			if order['quantity'] % MAX_VOLUME == 0: continue
			cancel_order(session, order['order_id'])
		return

	action, price, key = ("SELL", ask, "ask_orders") if data['position'] > 0 else ("BUY", bid, "bid_orders")
	order = build_order("LIMIT", r, price, key)
	
	oid = send_order(session, order)
	data[key].append((oid, price, data['tick'], r))

def send_order(session, order):
	if order['type'] == "MARKET": order['dry_run'] = 0
	resp = session.post("http://localhost:9999/v1/orders", params = order)
	if resp.status_code == 429:
		sleep(resp.json()['wait'] / 1_000)
		resp = session.post("http://localhost:9999/v1/orders", params = order)
	if resp.ok:
		return resp.json()['order_id']
	return ApiException("Auth Error. Check API Key")

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

def liquidate_position(session, data, isMarket = False):
	cancel_all_orders(session)
	orders = get_liquidating_orders(data, isMarket)
	for order in orders:
		send_order(session, order)

def cancel_order(session, order_id):
	resp = session.delete(f"http://localhost:9999/v1/orders/{order_id}")
	if resp.ok:
		print(f"Order {order_id} Canceled")
	return ApiException("Auth Error. Check API Key")

def cancel_all_orders(session):
	resp = session.post(f"http://localhost:9999/v1/commands/cancel?all=1")
	if resp.ok:
		print("Cacelled All Orders")
	return ApiException("Auth Error. Check API Key")

def cancel_old_orders(session, data, orders, open_orders):
	new_oids = []
	for (oid, price, _tick, qty) in orders:
		if oid not in open_orders: continue
		elif data['tick'] - _tick > OLD_ORDER_CALIBRATION:
			cancel_order(session, oid)
		else:
			new_oids.append((oid, price, _tick, qty))
	return new_oids

## Bid / Ask Functions
def vol_spreading(data, calibration):
	data['vol_spread'] = round(calibration * data['vol'] * data['mid'], 2)

def trend_skewing(data):
	ask = data['mid'] + data['vol_spread']
	bid = data['mid'] - data['vol_spread']

	trend = data['gtrend']
	vol = data['vol']
	
	data['trend_factor'] = np.exp(TREND_SKEW_CALIBRATION * trend)
	
	if data['gtrend_confidence'] > 1.5:
		
		ask = data['trend_factor'] * ask
		bid = data['trend_factor'] * bid

		data['trend_factor_mmm'] = data['trend_factor'] * data['mid'] - data['mid']

		if bid >= data['mid']:
			bid = data['mid']

		if ask <= data['mid']:
			ask = data['mid']

	return round(bid, 2), round(ask, 2)

def inventory_skewing(data, bid, ask):

	pos = data['position']
	F_tight = abs(pos // MAX_VOLUME) * TIGHT_INVENTORY_SKEW_CALIBRATION
	F_loose = abs(pos // MAX_VOLUME) * LOOSE_INVENTORY_SKEW_CALIBRATION

	if pos > 0:
		ask = data['mid'] + (ask - data['mid']) * (1 / (1 + F_tight))
		bid = data['mid'] - (1 + F_loose) * (data['mid'] - bid)
	elif pos < 0:
		ask = data['mid'] + (1 + F_loose) * (ask - data['mid'])
		bid = data['mid'] - (data['mid'] - bid) * (1 / (1 + F_tight))

	return round(bid, 2), round(ask, 2)

## Logging and Displays
def display(data):

	print("------------")
	print("Tick", data['tick'])
	print("Position", data['position'])
	print(f"({data['current_bid']}, {data['mid']}, {data['current_ask']})")
	print("Volatility", round(data['vol'] * 100, 4))
	print("Time-Factor", round(data['time_factor'], 4))
	print("Vol-Spread", data['vol_spread'])
	print("Trend Factor", data['trend_factor'])
	print("Trend Factor Mid-Minus-Mid", data['trend_factor_mmm'])
	print("G.Trend Confidence", data['gtrend_confidence'])
	print("Trend Confidence", data['trend_confidence'])
	# print("Open Bids")
	# for (oid, price, tick) in data['bid_orders']:
		# print(f"[BUY {MAX_VOLUME} @ {price}]. Age {data['tick'] - tick}")
	# print("Open Asks")
	# for (oid, price, tick) in data['ask_orders']:
		# print(f"[SELL {MAX_VOLUME} @ {price}]. Age {data['tick'] - tick}")

###################################################################################################

def main():

	tick = 0
	position = 0
	position_holding = 0

	with requests.Session() as session:
		session.headers.update(API_KEY)

		while data['tick'] != 299 and not shutdown:
			
			## Dont trade for first n ticks
			get_tick(session, data)
			if data['tick'] < START_TICK:
				continue

			## Liquidate everything with 10 ticks remaining
			if data['tick'] > END_TICK:
				liquidate_position(session, data, isMarket = True)
				break

			get_security_info(session, data)
			if data['position'] == 0 or np.sign(data['position']) == -1 * np.sign(position):
				position_holding = 0
				position = data['position']

			if data['tick'] != tick:
				
				###################################################################################
				## Data Collection

				get_price_history(session, data)
				get_time_and_sales(session, data)

				###################################################################################
				## Order Management

				open_orders = get_open_orders(session)
				open_bids = [order for order in open_orders if order['action'] == "BUY"]
				open_bid_oids = [order['order_id'] for order in open_bids]

				open_asks = [order for order in open_orders if order['action'] == "SELL"]
				open_ask_oids = [order['order_id'] for order in open_asks]

				data['bid_orders'] = cancel_old_orders(session, data, data['bid_orders'], open_bid_oids)
				data['ask_orders'] = cancel_old_orders(session, data, data['ask_orders'], open_ask_oids)

				###################################################################################
				## Bid / Ask Skewing

				vol_spreading(data, VOL_CALIBRATION)
				bid, ask = trend_skewing(data)
				bid, ask = inventory_skewing(data, bid, ask)
				data['current_bid'] = bid
				data['current_ask'] = ask

				###################################################################################
				## External Hedger

				if abs(data['position']) >= 0:
					position_holding += 1
				elif np.sign(data['position']) == -1 * np.sign(position):
					position_holding = 0
				else:
					position_holding = 0
				position = data['position']

				F = abs(data['position'] // MAX_VOLUME) * MAX_HOLDING_CALIBRATION
				if data['position'] != 0 and position_holding > MAX_HOLDING_PERIOD + 1 - F:
					print("Liquidating Positions", data['position'], position_holding, MAX_HOLDING_PERIOD + 1 - F)
					liquidate_position(session, data)
					tick = data['tick']
					continue

				###################################################################################
				## Order Placement

				noSimilarBid = all(
					abs(bid - order[1]) > data['vol_spread'] * ORDER_PROXIMITY_CALIBRATION
					for order in data['bid_orders']
					if (order[-1] % MAX_VOLUME) == 0
				)
				noSimilarAsk = all(
					abs(ask - order[1]) > data['vol_spread'] * ORDER_PROXIMITY_CALIBRATION
					for order in data['ask_orders']
					if (order[-1] % MAX_VOLUME) == 0
				)
				if (noSimilarBid or noSimilarAsk):
					execute_orders(session, data, bid, ask, open_bids, open_asks)

				display(data)
				tick = data['tick']

if __name__ == '__main__':

	signal.signal(signal.SIGINT, signal_handler)
	main()
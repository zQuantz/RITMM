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
VOL_LOOKBACK = 10

data = {
	'tick': 0,
	'position': 0,
	'bid_vwap': 0,
	'ask_vwap': 0,
	'vol': 0,
	'bid_ladder': pd.DataFrame(),
	'best_bid': 0,
	'best_ask': 0,
	'best_spread': 0,
	'mid': 0,
	'ask_ladder': pd.DataFrame(),
	'ohlc': pd.DataFrame(),
	"orders": [],
	"bid_order_id": None,
	"ask_order_id": None
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
	resp = session.get(f"http://localhost:9999/v1/securities/history?ticker=ALGO&limit={VOL_LOOKBACK + 1}")
	if resp.ok:
		df = pd.DataFrame(resp.json(), columns = HISTCOLS).sort_values('tick', ascending = True)
		vol = np.log(1 + df.close.pct_change()).dropna()
		data['vol'] = np.sqrt((vol * vol).mean()) ## Insert vol calcs here
		data['ohlc'] = df
		return data
	return ApiException("Auth Error. Check API Key")

def get_full_price_history(session):
	resp = session.get(f"http://localhost:9999/v1/securities/history?ticker=ALGO")
	if resp.ok:
		df = pd.DataFrame(resp.json(), columns = HISTCOLS).sort_values('tick', ascending = True)
		df.to_csv(DIR / "data/ohlc.csv", index=False)
	return ApiException("Auth Error. Check API Key")

TASCOLS = ['id', 'period', 'tick', 'price', 'quantity']
def get_full_time_and_sales(session):
	resp = session.get(f"http://localhost:9999/v1/securities/tas?ticker=ALGO")
	if resp.ok:
		df = pd.DataFrame(resp.json(), columns = TASCOLS).sort_values('tick', ascending = True)
		df.to_csv(DIR / "data/tas.csv", index=False)
		dfa = df.groupby('tick').agg({'quantity': ['mean', 'sum']}).reset_index()
		dfa.columns = ['tick', 'avg_trade_volume', 'total_volume']
		dfa.to_csv(DIR / "data/tasagg.csv", index=False)
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
		sleep(resp.json()['wait'])
		resp = session.post("http://localhost:9999/v1/orders", params = params)
	if resp.ok:
		data['orders'].append(resp.json()['order_id'])
	return ApiException("Auth Error. Check API Key")

def cancel_all_orders(session, data):
	if len(data['orders']) == 0: return
	resp = session.post(f"http://localhost:9999/v1/commands/cancel?ids={','.join(map(str, data['orders']))}")
	if resp.ok:
		cancelled_orders = resp.json()['cancelled_order_ids']
		data['orders'] = [
			oid
			for oid in data['orders']
			if oid not in data['orders']
		]
		return data
	return ApiException("Auth Error. Check API Key")

###################################################################################################

def main():

	best_bid, best_ask = 0, 0

	with requests.Session() as session:
		session.headers.update(API_KEY)

		while data['tick'] != 300:
			
			## Gather information
			get_tick(session, data)
			get_security_info(session, data)
			get_order_book(session, data)
			# get_price_history(session, data)

			if data['best_bid'] != best_bid or data['best_ask'] != best_ask:

				cancel_all_orders(session, data)
				send_order(session, data, "LIMIT", MAX_VOLUME, "BUY", data['best_bid'])
				send_order(session, data, "LIMIT", MAX_VOLUME, "SELL", data['best_ask'])
				best_bid = data['best_bid']
				best_ask = data['best_ask']

			# if data['tick'] != tick:

			# 	print(data['vol'])
			# 	print(data['ohlc'])

			# 	cancel_all_orders(session, data)
			# 	print(data['orders'])

			# 	bid_order_price = round(data['bid_vwap'], 2)
			# 	ask_order_price = round(data['ask_vwap'], 2)

			# 	send_order(session, data, "LIMIT", MAX_VOLUME, "BUY", bid_order_price)
			# 	# data['bid_order_id'] = data['orders'][-1]
			# 	send_order(session, data, "LIMIT", MAX_VOLUME, "SELL", ask_order_price)
			# 	# data['ask_order_id'] = data['orders'][-1]
			# 	print(data['orders'])

			# 	print(data['tick'])
			# 	print(round(data['bid_vwap'], 2))
			# 	print(round(data['ask_vwap'], 2))
			# 	print("-----------------------")
			# 	tick = data['tick']

		# get_full_time_and_sales(session)
		# get_full_price_history(session)

if __name__ == '__main__':

	signal.signal(signal.SIGINT, signal_handler)
	main()
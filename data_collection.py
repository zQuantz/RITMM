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

PORT = 10002

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
	print("Signal Received", signum)
	signal.signal(signal.SIGINT, signal.SIG_DFL)
	shutdown = True

def get_tick(session, data):
	resp = session.get("http://localhost:9999/v1/case")
	if resp.ok:
		data['tick'] = resp.json()['tick']
		return data
	return ApiException("Auth Error. Check API Key")

HISTCOLS = ['tick', 'open', 'high', 'low', 'close']
def get_full_price_history(session):
	resp = session.get(f"http://localhost:9999/v1/securities/history?ticker=ALGO")
	if resp.ok:
		df = pd.DataFrame(resp.json(), columns = HISTCOLS).sort_values('tick', ascending = True)
		df.to_csv(DIR / f"data/ohlc_{PORT}.csv", index=False)
	return ApiException("Auth Error. Check API Key")

TASCOLS = ['id', 'period', 'tick', 'price', 'quantity']
def get_full_time_and_sales(session):
	resp = session.get(f"http://localhost:9999/v1/securities/tas?ticker=ALGO")
	if resp.ok:
		df = pd.DataFrame(resp.json(), columns = TASCOLS).sort_values('tick', ascending = True)
		df.to_csv(DIR / f"data/tas_{PORT}.csv", index=False)
		dfa = df.groupby('tick').agg({'quantity': ['mean', 'sum']}).reset_index()
		dfa.columns = ['tick', 'avg_trade_volume', 'total_volume']
		dfa.to_csv(DIR / f"data/tasagg_{PORT}.csv", index=False)
	return ApiException("Auth Error. Check API Key")

###################################################################################################

def main():

	best_bid, best_ask, tick = 0, 0, 0

	with requests.Session() as session:
		session.headers.update(API_KEY)

		while data['tick'] != 299:
			
			get_tick(session, data)
			if data['tick'] != tick:
				print(data['tick'])
				tick = data['tick']

		get_full_time_and_sales(session)
		get_full_price_history(session)

if __name__ == '__main__':

	signal.signal(signal.SIGINT, signal_handler)
	main()
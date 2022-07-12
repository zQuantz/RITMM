from time import sleep
import requests
import signal
import sys

###################################################################################################

API_KEY = {'X-API-Key': '4RBO5VMI'}
shutdown = False

MAX_VOLUME = 5_000
MAX_POSITION = 25_000
MKT_COM = 0.01
LMT_COM = 0.005

BOOK_LIMIT = 40
VOL_LOOKBACK = 30

data = {
	'tick': 0,
	'position': 0,
	'bid_vwap': 0,
	'ask_vwap': 0,
	'vol': 0,
	'bid_ladder': pd.DataFrame(),
	'ask_ladder': pd.DataFrame(),
	'ohlc': pd.DataFrame(),
	"orders": []
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
		data['position'] = resp['position']
		return data
	return ApiException("Auth Error. Check API Key")

OBKEYS = ['price', 'quantity', 'quantity_filled']
def aggregate_order_book(items, isBid = True):
	df = pd.DataFrame(
		[
			{
				key: bid[key]
				for key in OBCOLS
			}
			for bid in items
			if bid['status'] == "OPEN"
		],
		columns = OBKEYS
	)
	df['quantity'] = df.quantity - df.quantity_filled
	df = df[df.quantity != 0].drop('quantity_filled', axis=1)
	df = df.groupby('quantity').sum().sort_index(ascending = !isBid).reset_index()
	vwap = (df.price * (df.quantity / df.quantity.sum())).sum()
	return df, vwap

def get_order_book(session, data):
	resp = session.get(f"http://localhost:9999/v1/securities?ticker=ALGO&limit={BOOK_LIMIT}")
	if resp.ok:
		resp = resp.json()
		data['bid_ladder'], data['bid_vwap'] = aggregate_order_book(resp['bid'])
		data['ask_ladder'], data['ask_vwap'] = aggregate_order_book(resp['ask'], isBid = False)
		return data
	return ApiException("Auth Error. Check API Key")


HISTCOLS = ['tick', 'open', 'high', 'low', 'close']
def get_price_history(session, data):
	resp = session.get(f"http://localhost:9999/v1/history?ticker=ALGO&limit={VOL_LOOKBACK + 1}")
	if resp.ok:
		df = pd.DataFrame(resp.json(), columns = HISTCOLS)
		vol = np.log(1 + df.close.pct_change()).dropna()
		data['vol'] = np.sqrt((vol * vol).mean()) ## Insert vol calcs here
		data['ohlc'] = df
		return data
	return ApiException("Auth Error. Check API Key")

## Subject to rate limits
def send_order(session, data, _type, quantity, action, price = 0):
	body = {
		'ticker': 'ALGO',
		'type': _type,
		'quantity': quantity,
		'action': action,
		'price': price,
	}
	if _type == "MARKET": body['dry_run'] = 0
	resp = session.post("http://localhost:9999/v1/orders", data = body)
	if response.status_code == 429:
		time.sleep(resp.json()['wait'])
		resp = session.post("http://localhost:9999/v1/orders", data = body)
	if resp.ok:
		data['orders'].append(resp.json()['order_id'])
	return ApiException("Auth Error. Check API Key")

def cancel_all_orders(session, data):
	if len(data['orders']) == 0: return
	resp = session.get(f"http://localhost:9999/v1/history?ids={','.join(data['orders'])}")
	if resp.ok:
		cancelled_orders = resp.json()['cancelled_order_ids']
		data['orders'] = [
			oid
			for oid in data['orders']
			if oid not in dat['orders']
		]
		return data
	return ApiException("Auth Error. Check API Key")

###################################################################################################

def main():

	tick = 0

	with requests.Session() as session:
		session.headers.update(API_KEY)
		while tick != 300:
			tick = get_tick(session)
			print(tick)

if __name__ == '__main__':

	signal.signal(signal.SIGINT, signal_handler)
	main()
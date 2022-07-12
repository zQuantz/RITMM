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

###################################################################################################

class ApiException(Exception):
	pass

def signal_handler(signum, frame):
	global shutdown
	signal.signal(signal.SIGINT, signal.SIG_DFL)
	shutdown = True

def get_tick(session):
	resp = session.get("http://localhost:9999/v1/case")
	if resp.ok: return resp.json()['tick']
	return ApiException("Auth Error. Check API Key")

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
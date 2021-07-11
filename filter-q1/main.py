#!/usr/bin/env python3

from common.utils.config_setup import setup
from src.query1_controller import Query1Controller

def main():
	config_params = setup('config.ini', {'MATCHES_EXCHANGE_NAME': False, 'OUTPUT_QUEUE': False, 'RABBIT_IP': False})
	rabbit_ip = config_params['RABBIT_IP']
	matches_exchange_name = config_params['MATCHES_EXCHANGE_NAME']
	output_queue = config_params['OUTPUT_QUEUE']

	controller1 = Query1Controller(rabbit_ip, matches_exchange_name, output_queue)
	controller1.run()

if __name__== "__main__":
	main()

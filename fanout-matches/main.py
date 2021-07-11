from common.utils.config_setup import setup
from common.controllers.fanout_controller import FanoutController

def main():
	config_params = setup('config.ini', {'INPUT_MATCHES_QUEUE': False, 'MATCHES_EXCHANGE_NAME': False, 'RABBIT_IP': False, 'MAX_BATCH_SIZE': True})
	rabbit_ip = config_params['RABBIT_IP']
	matches_queue = config_params['INPUT_MATCHES_QUEUE']
	exchange_name = config_params['MATCHES_EXCHANGE_NAME']
	max_batch_size = config_params['MAX_BATCH_SIZE']

	fanout = FanoutController(rabbit_ip, matches_queue, exchange_name, max_batch_size)
	fanout.run()

if __name__== "__main__":
	main()

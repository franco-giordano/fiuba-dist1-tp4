from common.utils.config_setup import setup
from common.controllers.fanout_controller import FanoutController

def main():
	config_params = setup('config.ini', {'INPUT_PLAYERS_QUEUE': False, 'PLAYERS_EXCHANGE_NAME': False, 'RABBIT_IP': False, 'MAX_BATCH_SIZE': True})
	rabbit_ip = config_params['RABBIT_IP']
	players_queue = config_params['INPUT_PLAYERS_QUEUE']
	exchange_name = config_params['PLAYERS_EXCHANGE_NAME']
	max_batch_size = config_params['MAX_BATCH_SIZE']

	fanout = FanoutController(rabbit_ip, players_queue, exchange_name, max_batch_size)
	fanout.run()

if __name__== "__main__":
	main()

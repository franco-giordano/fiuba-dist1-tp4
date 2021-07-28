from common.utils.config_setup import setup
# from common.controllers.shard_exchange_controller import ShardExchangeController
from src.cli_manager_controller import CliManagerController

def main():
	config_params = setup('config.ini',
		{'REQUESTS_QUEUE_NAME': False,
		'RABBIT_IP': False,
		'SYS_STATUS_FILENAME': False,
		'LAST_ROW_FILENAME': False})
	rabbit_ip = config_params['RABBIT_IP']
	requests_queue_name = config_params['REQUESTS_QUEUE_NAME']
	sys_status_filename = config_params['SYS_STATUS_FILENAME']

	controller = CliManagerController(rabbit_ip, requests_queue_name, sys_status_filename)
	controller.run()

if __name__== "__main__":
	main()

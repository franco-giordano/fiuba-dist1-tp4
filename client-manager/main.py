from common.utils.config_setup import setup
# from common.controllers.shard_exchange_controller import ShardExchangeController

def main():
	config_params = setup('config.ini',
		{'REQUESTS_QUEUE_NAME': False,
		'RABBIT_IP': False,
		'PING_QUEUE_NAME': False,
		'SYS_STATUS_FILENAME': False})
	rabbit_ip = config_params['RABBIT_IP']
	ping_queue_name = config_params['PING_QUEUE_NAME']
	requests_queue_name = config_params['REQUESTS_QUEUE_NAME']
	sys_status_filename = config_params['SYS_STATUS_FILENAME']

	controller = CliManagerController(rabbit_ip, ping_queue_name, requests_queue_name, sys_status_filename)
	controller.run()

if __name__== "__main__":
	main()

from common.utils.config_setup import setup
from common.controllers.filter_matches_controller import FilterMatchesController
from src.filter_query3 import FilterQuery3
from multiprocessing import Process

def main():
	config_params = setup('config.ini',
		{'MATCHES_BY_LADDER_EXCHANGE_NAME': False,
		'OUTPUT_EXCHANGE_NAME': False,
		'RABBIT_IP': False,
		'NEXT_REDUCERS_AMOUNT': True,
		'1v1_ROUTING_KEY': False,
		'WORKERS_AMOUNT': True,
		'WORKERS_SHARED_QUEUE': False})
	workers_amount = config_params['WORKERS_AMOUNT']

	workers_proc = []

	for i in range(workers_amount):
		pr = Process(target=worker_init, args=(i, config_params))
		workers_proc.append(pr)
		pr.start()

	for p in workers_proc:
		p.join()

def worker_init(proc_id, config_params):
	rabbit_ip = config_params['RABBIT_IP']
	matches_exchange_name = config_params['MATCHES_BY_LADDER_EXCHANGE_NAME']
	output_exchange_name = config_params['OUTPUT_EXCHANGE_NAME']
	reducers_amount = config_params['NEXT_REDUCERS_AMOUNT']
	input_routing_key = config_params['1v1_ROUTING_KEY']
	batch_size = 1 # config_params['BATCH_SIZE']
	queue_name = config_params['WORKERS_SHARED_QUEUE']

	match_filter = FilterQuery3()
	controller = FilterMatchesController(rabbit_ip, matches_exchange_name, \
		output_exchange_name, reducers_amount, input_routing_key, batch_size, match_filter, queue_name)
	controller.run()

if __name__== "__main__":
	main()

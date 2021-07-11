from common.utils.config_setup import setup
from multiprocessing import Process
from src.sharded_grouper_controller import ShardedGrouperController

def main():
	config_params = setup('config.ini',
		{'SHARD_EXCHANGE_NAME': False,
		'OUTPUT_QUEUE_NAME': False,
		'RABBIT_IP': False,
		'REDUCERS_AMOUNT': True})
	rabbit_ip = config_params['RABBIT_IP']
	shard_exchange_name = config_params['SHARD_EXCHANGE_NAME']
	reducers_amount = config_params['REDUCERS_AMOUNT']
	output_queue_name = config_params['OUTPUT_QUEUE_NAME']

	reducers_proc = []

	for i in range(reducers_amount):
		pr = Process(target=reducer_init, args=(i, rabbit_ip, shard_exchange_name, output_queue_name))
		reducers_proc.append(pr)
		pr.start()

	for p in reducers_proc:
		p.join()

def reducer_init(proc_id, rabbit_ip, shard_exchange_name, output_queue_name):
	shard_key = str(proc_id)
	grouper = ShardedGrouperController(rabbit_ip, shard_exchange_name, output_queue_name, shard_key)
	grouper.run()

if __name__== "__main__":
	main()

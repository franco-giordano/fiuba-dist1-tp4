from common.utils.config_setup import setup
from multiprocessing import Process
from common.controllers.sharded_joiner_controller import ShardedJoinerController

def main():
	config_params = setup('config.ini',
		{'SHARD_EXCHANGE_NAME': False,
		'OUTPUT_EXCHANGE_NAME': False,
		'RABBIT_IP': False,
		'REDUCERS_AMOUNT': True,
		'NEXT_REDUCERS_AMOUNT': True,
		'TOTAL_INCOMING_SENTINELS': True})
	reducers_amount = config_params['REDUCERS_AMOUNT']

	reducers_proc = []

	for i in range(reducers_amount):
		pr = Process(target=reducer_init, args=(i, config_params))
		reducers_proc.append(pr)
		pr.start()

	for p in reducers_proc:
		p.join()

def reducer_init(proc_id, config_params):
	rabbit_ip = config_params['RABBIT_IP']
	shard_exchange_name = config_params['SHARD_EXCHANGE_NAME']
	output_exchange_name = config_params['OUTPUT_EXCHANGE_NAME']
	next_reducers_amount = config_params['NEXT_REDUCERS_AMOUNT']
	total_incoming_sentinels = config_params['TOTAL_INCOMING_SENTINELS']

	shard_key = str(proc_id)
	joiner = ShardedJoinerController(rabbit_ip, shard_exchange_name, output_exchange_name, \
		shard_key, next_reducers_amount, total_incoming_sentinels, force_send=True)
	joiner.run()

if __name__== "__main__":
	main()

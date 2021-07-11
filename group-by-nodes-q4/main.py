from common.utils.config_setup import setup
from multiprocessing import Process
from common.controllers.sharded_grouper_controller import ShardedGrouperController
from common.models.total_uses_aggregator import TotalUsesAggregator

def main():
	config_params = setup('config.ini',
		{'SHARD_EXCHANGE_NAME': False,
		'OUTPUT_QUEUE_NAME': False,
		'RABBIT_IP': False,
		'REDUCERS_AMOUNT': True,
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
	output_queue_name = config_params['OUTPUT_QUEUE_NAME']
	total_incoming_sentinels = config_params['TOTAL_INCOMING_SENTINELS']

	shard_key = str(proc_id)
	aggregator = TotalUsesAggregator()
	grouper = ShardedGrouperController(rabbit_ip, shard_exchange_name, output_queue_name, shard_key, aggregator, total_incoming_sentinels)
	grouper.run()

if __name__== "__main__":
	main()

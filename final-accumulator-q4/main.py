from common.utils.config_setup import setup
from common.controllers.accumulator_controller import AccumulatorController
from common.models.top5_counts_accumulator import Top5CountsAccumulator

def main():
	config_params = setup('config.ini',
		{'PARTIAL_USAGE_QUEUE_NAME': False,
		'OUTPUT_QUEUE_NAME': False,
		'RABBIT_IP': False,
		'TOTAL_INCOMING_SENTINELS': True})
	rabbit_ip = config_params['RABBIT_IP']
	input_queue_name = config_params['PARTIAL_USAGE_QUEUE_NAME']
	output_queue = config_params['OUTPUT_QUEUE_NAME']
	total_incoming_sentinels = config_params['TOTAL_INCOMING_SENTINELS']

	accum = Top5CountsAccumulator()
	controller = AccumulatorController(rabbit_ip, input_queue_name, \
		output_queue, total_incoming_sentinels, accum)
	controller.run()

if __name__== "__main__":
	main()

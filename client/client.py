from common.utils.config_setup import setup
from src.csv_dispatcher import CSVDispatcher
from src.results_controller import ResultsController
from multiprocessing import Process

def main():
	config_params = setup('config.ini',
		{'INPUT_MATCHES_QUEUE': False,
		'RABBIT_IP': False,
		'INPUT_PLAYERS_QUEUE': False,
		'MATCHES_PATH': False,
		'PLAYERS_PATH': False,
		'BATCH_SIZE': True,
		'RESULTS_1': False,
		'RESULTS_2': False,
		'RESULTS_3': False,
		'RESULTS_4': False,
		'OUTPUT_FILE': False})

	rabbit_ip = config_params['RABBIT_IP']
	matches_queue = config_params['INPUT_MATCHES_QUEUE']
	matches_path = config_params['MATCHES_PATH']
	players_queue = config_params['INPUT_PLAYERS_QUEUE']
	players_path = config_params['PLAYERS_PATH']
	batch_size = config_params['BATCH_SIZE']

	dispatcher = CSVDispatcher(rabbit_ip, matches_queue, matches_path, players_queue, players_path, batch_size)

	proc_res = Process(target=results_init, args=(config_params,))
	proc_res.start()

	dispatcher.run()

	proc_res.join()

def results_init(config_params):
	rabbit_ip = config_params['RABBIT_IP']
	results1 = config_params['RESULTS_1']
	results2 = config_params['RESULTS_2']
	results3 = config_params['RESULTS_3']
	results4 = config_params['RESULTS_4']
	output_file_name = config_params['OUTPUT_FILE']

	with open(f'/results/{output_file_name}', 'w') as res_file:
		results = ResultsController(rabbit_ip, results1, results2, results3, results4, res_file)
		results.run()

if __name__== "__main__":
	main()

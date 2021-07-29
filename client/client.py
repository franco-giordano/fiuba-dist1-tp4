from multiprocessing import Process

from common.utils.config_setup import setup
from src.csv_dispatcher import CSVDispatcher
from src.results_controller import ResultsController


def main():
    config_params = setup('config.ini',
                          {'INPUT_MATCHES_QUEUE': False,
                           'RABBIT_IP': False,
                           'INPUT_PLAYERS_QUEUE': False,
                           'MATCHES_PATH': False,
                           'PLAYERS_PATH': False,
                           'ID': False
                           })

    node_id = config_params['ID']
    rabbit_ip = config_params['RABBIT_IP']
    matches_queue = config_params['INPUT_MATCHES_QUEUE']
    matches_path = config_params['MATCHES_PATH']
    players_queue = config_params['INPUT_PLAYERS_QUEUE']
    players_path = config_params['PLAYERS_PATH']
    batch_size = 1  # config_params['BATCH_SIZE']

    dispatcher = CSVDispatcher(node_id, rabbit_ip, matches_queue,
                               matches_path, players_queue, players_path, batch_size)

    results_controllers = [Process(target=results_init, args=(
        config_params, i)) for i in range(1, 5)]

    able_to_upload = dispatcher.run()

    if not able_to_upload:
        return  # No tengo que esperar respuesta ni notificar que el sistema esta libre, porque yo no lo pude usar

    for handler in results_controllers:
        handler.start()

    for handler in results_controllers:
        handler.join()

    dispatcher.finished_proccessing()


def results_init(config_params, controller_index):
    rabbit_ip = config_params['RABBIT_IP']
    node_id = config_params['ID']
    partial_persistance_filename = f"/persistance/{node_id}-persistance-query{controller_index}"
    result_file = f'/persistance/{node_id}-output{controller_index}'
    results = ResultsController(controller_index, rabbit_ip, f"output-query{controller_index}",
                                partial_persistance_filename, result_file)
    results.run()


if __name__ == "__main__":
    main()

from common.controllers.filter_matches_controller import FilterMatchesController
from common.utils.config_setup import setup
from src.filter_query4 import FilterQuery4


def main():
    config_params = setup('config.ini',
                          {'MATCHES_BY_LADDER_EXCHANGE_NAME': False,
                           'OUTPUT_EXCHANGE_NAME': False,
                           'RABBIT_IP': False,
                           'NEXT_REDUCERS_AMOUNT': True,
                           'TEAM_ROUTING_KEY': False,
                           'WORKERS_SHARED_QUEUE': False,
                           'NODE_NAME': False,
                           'PONGS_QUEUE': False})

    worker_init(config_params)


def worker_init(config_params):
    rabbit_ip = config_params['RABBIT_IP']
    matches_exchange_name = config_params['MATCHES_BY_LADDER_EXCHANGE_NAME']
    output_exchange_name = config_params['OUTPUT_EXCHANGE_NAME']
    reducers_amount = config_params['NEXT_REDUCERS_AMOUNT']
    input_routing_key = config_params['TEAM_ROUTING_KEY']
    batch_size = 1  # config_params['BATCH_SIZE']
    queue_name = config_params['WORKERS_SHARED_QUEUE']
    pongs_queue = config_params['PONGS_QUEUE']
    node_name = config_params['NODE_NAME']

    match_filter = FilterQuery4()
    controller = FilterMatchesController(node_name, rabbit_ip, matches_exchange_name, \
                                         output_exchange_name, pongs_queue, reducers_amount, input_routing_key,
                                         batch_size, match_filter, queue_name)
    controller.run()


if __name__ == "__main__":
    main()

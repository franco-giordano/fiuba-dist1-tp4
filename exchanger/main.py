from common.controllers.shard_exchange_controller import ShardExchangeController
from common.utils.config_setup import setup


def main():
    config_params = setup('config.ini',
                          {'PLAYERS_EXCHANGE_NAME': False,
                           'OUTPUT_EXCHANGE_NAME': False,
                           'RABBIT_IP': False,
                           'NEXT_REDUCERS_AMOUNT': True,
                           'PONGS_QUEUE': False,
                           'NODE_NAME': False})
    rabbit_ip = config_params['RABBIT_IP']
    players_exchange_name = config_params['PLAYERS_EXCHANGE_NAME']
    output_exchange_name = config_params['OUTPUT_EXCHANGE_NAME']
    reducers_amount = config_params['NEXT_REDUCERS_AMOUNT']
    pongs_queue = config_params['PONGS_QUEUE']
    node_name = config_params['NODE_NAME']
    batch_size = 1  # config_params['BATCH_SIZE']

    controller = ShardExchangeController(node_name, rabbit_ip, players_exchange_name, output_exchange_name,
                                         reducers_amount, pongs_queue, batch_size)
    controller.run()


if __name__ == "__main__":
    main()

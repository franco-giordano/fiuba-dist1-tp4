from common.controllers.sharded_joiner_controller import ShardedJoinerController
from common.utils.config_setup import setup


def main():
    config_params = setup('config.ini',
                          {'SHARD_EXCHANGE_NAME': False,
                           'OUTPUT_EXCHANGE_NAME': False,
                           'RABBIT_IP': False,
                           'NEXT_REDUCERS_AMOUNT': True,
                           'JOINER_ID': True,
                           'SENTINELS_FILE_BASE': False,
                           'PERSISTANCE_FILE_BASE': False,
                           'NODE_NAME': False,
                           'PONGS_QUEUE': False})
    my_id = config_params['JOINER_ID']
    reducer_init(my_id, config_params)


def reducer_init(proc_id, config_params):
    rabbit_ip = config_params['RABBIT_IP']
    shard_exchange_name = config_params['SHARD_EXCHANGE_NAME']
    output_exchange_name = config_params['OUTPUT_EXCHANGE_NAME']
    next_reducers_amount = config_params['NEXT_REDUCERS_AMOUNT']
    total_incoming_sentinels = 2  # config_params['TOTAL_INCOMING_SENTINELS']
    persistance_file = f'{config_params["PERSISTANCE_FILE_BASE"]}{proc_id}'
    sentinels_file = f'{config_params["SENTINELS_FILE_BASE"]}{proc_id}'
    node_name = config_params['NODE_NAME']
    pongs_queue = config_params['PONGS_QUEUE']

    shard_key = str(proc_id)
    joiner = ShardedJoinerController(node_name, rabbit_ip, shard_exchange_name, output_exchange_name, pongs_queue,
                                     shard_key, next_reducers_amount, total_incoming_sentinels,
                                     persistance_file, sentinels_file)
    joiner.run()


if __name__ == "__main__":
    main()

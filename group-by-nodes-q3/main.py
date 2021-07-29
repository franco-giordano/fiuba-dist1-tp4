from common.controllers.sharded_grouper_controller import ShardedGrouperController
from common.models.victories_total_aggregator import VictoriesTotalAggregator
from common.utils.config_setup import setup


def main():
    config_params = setup('config.ini',
                          {'SHARD_EXCHANGE_NAME': False,
                           'OUTPUT_QUEUE_NAME': False,
                           'RABBIT_IP': False,
                           'REDUCERS_AMOUNT': True,
                           'GROUPER_ID': True,
                           'TOTAL_INCOMING_SENTINELS': True,
                           'PERSISTANCE_FILENAME': False,
                           'PONGS_QUEUE': False,
                           'NODE_NAME': False})

    my_id = config_params['GROUPER_ID']

    reducer_init(my_id, config_params)


def reducer_init(proc_id, config_params):
    rabbit_ip = config_params['RABBIT_IP']
    shard_exchange_name = config_params['SHARD_EXCHANGE_NAME']
    output_queue_name = config_params['OUTPUT_QUEUE_NAME']
    total_incoming_sentinels = config_params['TOTAL_INCOMING_SENTINELS']
    persistance_filename = config_params['PERSISTANCE_FILENAME']
    pongs_queue = config_params['PONGS_QUEUE']
    node_name = config_params['NODE_NAME']

    shard_key = str(proc_id)
    aggregator = VictoriesTotalAggregator()
    grouper = ShardedGrouperController(node_name, rabbit_ip, shard_exchange_name, output_queue_name, pongs_queue, shard_key, aggregator,
                                       total_incoming_sentinels, persistance_filename)
    grouper.run()


if __name__ == "__main__":
    main()

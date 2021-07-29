from common.utils.config_setup import setup
from src.sharded_grouper_controller import ShardedGrouperController


def main():
    config_params = setup('config.ini',
                          {'SHARD_EXCHANGE_NAME': False,
                           'OUTPUT_QUEUE_NAME': False,
                           'RABBIT_IP': False,
                           'REDUCERS_AMOUNT': True,
                           'GROUPER_ID': True,
                           'TOTAL_INCOMING_SENTINELS': True,
                           'PERSISTANCE_FILENAME': False})

    grouper_id = config_params['GROUPER_ID']
    rabbit_ip = config_params['GROUPER_ID']
    shard_exchange_name = config_params['GROUPER_ID']
    output_queue_name = config_params['GROUPER_ID']
    grouper_id = config_params['GROUPER_ID']

    reducer_init()


def reducer_init(proc_id, config_params):
    rabbit_ip = config_params['RABBIT_IP']
    shard_exchange_name = config_params['SHARD_EXCHANGE_NAME']
    output_queue_name = config_params['OUTPUT_QUEUE_NAME']
    total_incoming_sentinels = config_params['TOTAL_INCOMING_SENTINELS']
    persistance_filename = config_params['PERSISTANCE_FILENAME']
    shard_key = str(proc_id)

    grouper = ShardedGrouperController(rabbit_ip, shard_exchange_name, output_queue_name, shard_key)
    grouper.run()


if __name__ == "__main__":
    main()

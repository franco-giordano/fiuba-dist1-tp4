from common.utils.config_setup import setup
from src.sharded_grouper_controller import ShardedGrouperController


def main():
    config_params = setup('config.ini',
                          {'SHARD_EXCHANGE_NAME': False,
                           'OUTPUT_QUEUE_NAME': False,
                           'RABBIT_IP': False,
                           'GROUPER_ID': True,
                           'PERSISTANCE_FILENAME': False})

    proc_id = str(config_params['GROUPER_ID'])

    reducer_init(proc_id, config_params)


def reducer_init(proc_id, config_params):
    rabbit_ip = config_params['RABBIT_IP']
    shard_exchange_name = config_params['SHARD_EXCHANGE_NAME']
    output_queue_name = config_params['OUTPUT_QUEUE_NAME']
    persistance_file = config_params['PERSISTANCE_FILENAME']

    grouper = ShardedGrouperController(proc_id, rabbit_ip, shard_exchange_name, output_queue_name, persistance_file)
    grouper.run()


if __name__ == "__main__":
    main()

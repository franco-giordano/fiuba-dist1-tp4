from common.controllers.sharded_joiner_controller import ShardedJoinerController
from common.utils.config_setup import setup


def main():
    config_params = setup('config.ini',
                          {'SHARD_EXCHANGE_NAME': False,
                           'OUTPUT_EXCHANGE_NAME': False,
                           'RABBIT_IP': False,
                           'NEXT_REDUCERS_AMOUNT': True,
                           'TOTAL_INCOMING_SENTINELS': True,
                           'JOINER_ID': True,
                           'SENTINELS_FILE_BASE': False,
                           'PERSISTANCE_FILE_BASE': False})
    my_id = config_params['JOINER_ID']
    reducer_init(my_id, config_params)


# reducers_proc = []

# for i in range(reducers_amount):
# 	pr = Process(target=reducer_init, args=(i, config_params))
# 	reducers_proc.append(pr)
# 	pr.start()

# _initialize_log(str(-99))
# logging.info(f'@MAIN: ############ WAITING...')

# for p in reducers_proc:
# 	p.join()
# 	logging.info(f'@MAIN: ############ PROCESS NUMBER {p} FINISHED!')

def reducer_init(proc_id, config_params):
    rabbit_ip = config_params['RABBIT_IP']
    shard_exchange_name = config_params['SHARD_EXCHANGE_NAME']
    output_exchange_name = config_params['OUTPUT_EXCHANGE_NAME']
    next_reducers_amount = config_params['NEXT_REDUCERS_AMOUNT']
    total_incoming_sentinels = config_params['TOTAL_INCOMING_SENTINELS']
    persistance_file = f'{config_params["PERSISTANCE_FILE_BASE"]}{proc_id}'
    sentinels_file = f'{config_params["SENTINELS_FILE_BASE"]}{proc_id}'

    shard_key = str(proc_id)
    joiner = ShardedJoinerController(rabbit_ip, shard_exchange_name, output_exchange_name,
                                     shard_key, next_reducers_amount, total_incoming_sentinels,
                                     persistance_file, sentinels_file)
    joiner.run()

if __name__ == "__main__":
    main()

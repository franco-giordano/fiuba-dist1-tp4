from common.utils.config_setup import setup
from src.master_controller import MasterController
from src.pings_controller import PingsController
from multiprocessing import Process


def main():
    config_params = setup('config.ini', {
                          'RABBIT_IP': False,
                          'MY_MASTER_ID': True,
                          'MASTERS_AMOUNT': True,
                          'PONGS_QUEUE': False,
                          'MASTER_COMMS_EXCH': False,
                          'LOG_FILENAME': False,
                          'NODES_LIST': False,
                          })
    rabbit_ip = config_params['RABBIT_IP']
    master_comms = config_params['MASTER_COMMS_EXCH']
    my_master_id = config_params['MY_MASTER_ID']
    masters_amount = config_params['MASTERS_AMOUNT']
    log_filename = config_params['LOG_FILENAME']

    controller = MasterController(
        rabbit_ip, master_comms, my_master_id, masters_amount)

    pings_proc = Process(target=pings_init, args=(config_params,))
    if my_master_id == 0:
        pings_proc.start()

    controller.run()
    pings_proc.join()


def pings_init(config_params):
    rabbit_ip = config_params['RABBIT_IP']
    pongs_queue = config_params['PONGS_QUEUE']
    log_filename = config_params['LOG_FILENAME']
    nodes_list = config_params['NODES_LIST'].split(',')


    controller = PingsController(rabbit_ip, pongs_queue, nodes_list, log_filename)
    controller.run()


if __name__ == "__main__":
    main()

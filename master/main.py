from common.utils.config_setup import setup
from src.master_controller import MasterController


def main():
    config_params = setup('config.ini', {
                          'RABBIT_IP': False,
                          'MY_MASTER_ID': True,
                          'MASTERS_AMOUNT': True,
                          'PINGS_FANOUT': False,
                          'PONGS_QUEUE': False,
                          'MASTER_COMMS_EXCH': False})
    rabbit_ip = config_params['RABBIT_IP']
    master_comms = config_params['MASTER_COMMS_EXCH']
    my_master_id = config_params['MY_MASTER_ID']
    masters_amount = config_params['MASTERS_AMOUNT']
    pings_fanout = config_params['PINGS_FANOUT']
    pongs_queue = config_params['PONGS_QUEUE']

    controller = MasterController(
        rabbit_ip, master_comms, my_master_id, masters_amount, pings_fanout, pongs_queue)
    controller.run()


if __name__ == "__main__":
    main()

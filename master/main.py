from common.utils.config_setup import setup
from src.master_controller import MasterController


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

    controller = MasterController(
        rabbit_ip, master_comms, my_master_id, masters_amount)

    controller.run()


if __name__ == "__main__":
    main()

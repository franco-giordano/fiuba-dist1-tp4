import logging
from common.controllers.accumulator_controller import AccumulatorController
from common.models.top5_counts_accumulator import Top5CountsAccumulator
from common.utils.config_setup import setup


def main():
    config_params = setup('config.ini',
                          {'PARTIAL_USAGE_QUEUE_NAME': False,
                           'OUTPUT_QUEUE_NAME': False,
                           'RABBIT_IP': False,
                           'TOTAL_INCOMING_SENTINELS': True,
                           'PONGS_QUEUE': False,
                           'NODE_NAME': False})
    rabbit_ip = config_params['RABBIT_IP']
    input_queue_name = config_params['PARTIAL_USAGE_QUEUE_NAME']
    output_queue = config_params['OUTPUT_QUEUE_NAME']
    total_incoming_sentinels = config_params['TOTAL_INCOMING_SENTINELS']
    node_name = config_params['NODE_NAME']
    pongs_queue = config_params['PONGS_QUEUE']

    logging.info(f"{node_name}, {pongs_queue}")

    accum = Top5CountsAccumulator()
    controller = AccumulatorController(node_name, rabbit_ip, input_queue_name,
                                       output_queue, pongs_queue, total_incoming_sentinels, accum)
    controller.run()


if __name__ == "__main__":
    main()

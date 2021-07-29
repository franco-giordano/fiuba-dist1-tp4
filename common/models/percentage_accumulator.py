from common.encoders.batch_encoder_decoder import BatchEncoderDecoder
import logging
from common.encoders.api_pkts_encoder_decoder import ApiPacketsEncoder
from common.utils.rabbit_utils import RabbitUtils

class PercentageAccumulator:
    def __init__(self):
        self.accum = {}

    def add_partial_data(self, data):
        civ, pair = data
        logging.info(f"PERCENTAGE ACCUMULATOR: Received partial count for civ {civ} data {pair}")
        current_victories_total = self.accum.get(civ, [0,0])
        current_victories_total[0] += pair[0]
        current_victories_total[1] += pair[1]
        self.accum[civ] = current_victories_total

    def flush_results(self, channel, queue_name):
        ini_msg = ApiPacketsEncoder.create_inicio_pkt()
        RabbitUtils.send_to_queue(channel, queue_name, ini_msg)

        for civ, pair in self.accum.items():
            result = [civ, pair[0] / pair[1]]
            serialized = BatchEncoderDecoder.encode_batch(result)
            logging.info(f"PERCENTAGE ACCUMULATOR: Sending result {serialized}")
            RabbitUtils.send_to_queue(channel, queue_name, serialized)

        fin_msg = ApiPacketsEncoder.create_fin_pkt()
        RabbitUtils.send_to_queue(channel, queue_name, fin_msg)

        self.accum = {}

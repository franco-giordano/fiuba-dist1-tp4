from common.encoders.batch_encoder_decoder import BatchEncoderDecoder
import logging
from common.encoders.api_pkts_encoder_decoder import ApiPacketsEncoder
from common.utils.rabbit_utils import RabbitUtils

class Top5CountsAccumulator:
    def __init__(self):
        self.accum = {}

    def add_partial_data(self, data):
        civ, count = data
        logging.info(f"TOP 5 ACCUMULATOR: Received partial count for civ {civ} count {count}")
        current_usage_count = self.accum.get(civ, 0)
        current_usage_count += count
        self.accum[civ] = current_usage_count

    def flush_results(self, channel, queue_name):
        ini_msg = ApiPacketsEncoder.create_inicio_pkt()
        RabbitUtils.send_to_queue(channel, queue_name, ini_msg)

        top_5_civs = sorted(self.accum, key=self.accum.get, reverse=True)[:5]
        logging.info(f"TOP 5 ACCUMULATOR: Found top 5 civs {top_5_civs}")
        for civ in top_5_civs:
            result = [civ, self.accum[civ]]
            serialized = BatchEncoderDecoder.encode_batch(result)
            logging.info(f"TOP 5 ACCUMULATOR: Sending result {serialized}")
            RabbitUtils.send_to_queue(channel, queue_name, serialized)

        fin_msg = ApiPacketsEncoder.create_fin_pkt()
        RabbitUtils.send_to_queue(channel, queue_name, fin_msg)

        self.accum = {}

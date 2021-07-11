from common.encoders.batch_encoder_decoder import BatchEncoderDecoder
import logging

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
        for civ, pair in self.accum.items():
            result = [civ, pair[0] / pair[1]]
            serialized = BatchEncoderDecoder.encode_batch(result)
            logging.info(f"PERCENTAGE ACCUMULATOR: Sending result {serialized}")
            channel.basic_publish(exchange='', routing_key=queue_name, body=serialized)

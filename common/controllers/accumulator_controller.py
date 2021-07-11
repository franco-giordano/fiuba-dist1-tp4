from common.encoders.batch_encoder_decoder import BatchEncoderDecoder
from common.utils.rabbit_utils import RabbitUtils
from common.models.sentinel_tracker import SentinelTracker
import logging

class AccumulatorController:
    def __init__(self, rabbit_ip, input_queue_name, output_queue_name, total_incoming_sentinels, accumulator):
        self.input_queue_name = input_queue_name
        self.output_queue_name = output_queue_name

        self.connection, self.channel = RabbitUtils.setup_connection_with_channel(rabbit_ip)

        # input exchange
        RabbitUtils.setup_input_queue(self.channel, self.input_queue_name, self._callback)

        # output queue
        RabbitUtils.setup_queue(self.channel, self.output_queue_name)

        self.accum = accumulator
        self.sentinel_tracker = SentinelTracker(total_incoming_sentinels)

    def run(self):
        logging.info('ACCUMULATOR: Waiting for messages. To exit press CTRL+C')
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logging.warning('ACCUMULATOR: ######### Received Ctrl+C! Stopping...')
            self.channel.stop_consuming()
        self.connection.close()

    def _callback(self, ch, method, properties, body):
        if BatchEncoderDecoder.is_encoded_sentinel(body):
            logging.info(f"ACCUMULATOR: Received one sentinel!")
            if self.sentinel_tracker.count_and_reached_limit():
                logging.info(f"ACCUMULATOR: Received all sentinels! Sending results and shutting down...")
                self.accum.flush_results(self.channel, self.output_queue_name)
                raise KeyboardInterrupt
            return

        data = BatchEncoderDecoder.decode_bytes(body)
        logging.info(f"ACCUMULATOR: Received partial data pair {body}...")
        self.accum.add_partial_data(data)

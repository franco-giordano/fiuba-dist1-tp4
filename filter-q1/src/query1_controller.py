from common.encoders.batch_encoder_decoder import BatchEncoderDecoder
from src.filter_query1 import FilterQuery1
from common.utils.rabbit_utils import RabbitUtils
import logging

class Query1Controller:
    def __init__(self, rabbit_ip, matches_exchange_name, output_queue_name):
        self.matches_exchange_name = matches_exchange_name
        self.output_queue_name = output_queue_name

        self.connection, self.channel = RabbitUtils.setup_connection_with_channel(rabbit_ip)

        # input exchange
        RabbitUtils.setup_input_fanout_exchange(self.channel, self.matches_exchange_name, self._callback)

        # output queue
        RabbitUtils.setup_queue(self.channel, self.output_queue_name)

        self.filter = FilterQuery1()

    def run(self):
        logging.info('FILTER QUERY1: Waiting for messages. To exit press CTRL+C')
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logging.warning('FILTER QUERY1: ######### Received Ctrl+C! Stopping...')
            self.channel.stop_consuming()
        self.connection.close()

    def _callback(self, ch, method, properties, body):
        if BatchEncoderDecoder.is_encoded_sentinel(body):
            logging.info(f"FILTER QUERY1: Received sentinel! Shutting down...")
            raise KeyboardInterrupt

        # TODO: hacer la logica para que borre duplicados y mande al final

        batch = BatchEncoderDecoder.decode_bytes(body)
        logging.info(f"FILTER QUERY1: Received batch {body[:25]}...")
        passing = list(filter(self.filter.should_pass, batch))

        if passing:
            logging.info(f'FILTER QUERY1: Sending to output queue the passing matches {passing}')
            serialized = BatchEncoderDecoder.encode_batch(passing)
            self.channel.basic_publish(exchange='', routing_key=self.output_queue_name, body=serialized)

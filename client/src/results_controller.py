from common.encoders.batch_encoder_decoder import BatchEncoderDecoder
from common.utils.rabbit_utils import RabbitUtils
import logging

class ResultsController:
    def __init__(self, rabbit_ip, queue1, queue2, queue3, queue4, opened_file):
        self.opened_file = opened_file

        self.connection, self.channel = RabbitUtils.setup_connection_with_channel(rabbit_ip)

        # results queues
        RabbitUtils.setup_input_queue(self.channel, queue1, self._callback)
        RabbitUtils.setup_input_queue(self.channel, queue2, self._callback)
        RabbitUtils.setup_input_queue(self.channel, queue3, self._callback)
        RabbitUtils.setup_input_queue(self.channel, queue4, self._callback)

    def run(self):
        logging.info(f'RESULTS: Waiting for results. To exit press CTRL+C. Will write to {self.opened_file}')
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logging.warning('RESULTS: ######### Received Ctrl+C! Stopping...')
            self.channel.stop_consuming()
        self.connection.close()

    def _callback(self, ch, method, properties, body):        
        batch = BatchEncoderDecoder.decode_bytes(body)
        logging.info(f"RESULTS: Received batch, writing result '{method.routing_key}, {batch}\n'")
        self.opened_file.write(f'{method.routing_key}, {batch}\n')
        self.opened_file.flush()

import pika
from common.utils.rabbit_utils import RabbitUtils
from common.encoders.batch_encoder_decoder import BatchEncoderDecoder
from common.models.partition_batcher import PartitionBatcher
import logging

class FanoutController:
    def __init__(self, rabbit_ip, sources_queue_name, exchange_name, max_batch_size):
        self.exchange_name = exchange_name
        self.sources_queue_name = sources_queue_name

        self.connection, self.channel = RabbitUtils.setup_connection_with_channel(rabbit_ip)

        # setup input queue
        RabbitUtils.setup_input_queue(self.channel, self.sources_queue_name, self._callback)

        # setup output exchange
        RabbitUtils.setup_fanout_exchange(self.channel, self.exchange_name)
        self.partition_batcher = PartitionBatcher(self.channel, self.exchange_name, max_batch_size)

    def run(self):
        logging.info('FANOUT: Waiting for messages. To exit press CTRL+C')
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logging.warning('FANOUT: ######### Received Ctrl+C! Stopping...')
            self.channel.stop_consuming()
        self.connection.close()

    def _callback(self, ch, method, properties, body):
        if BatchEncoderDecoder.is_encoded_sentinel(body):
            logging.info(f"FANOUT: Received sentinel! Shutting down...")
            self.partition_batcher.received_sentinel()
            raise KeyboardInterrupt

        batch = BatchEncoderDecoder.decode_bytes(body)
        self.partition_batcher.send_batch(batch)

        # logging.info(f"FANOUT: Received batch {body[:25]}...")
        # is_sentinel = BatchEncoderDecoder.is_encoded_sentinel(body)

        # self.channel.basic_publish(exchange=self.exchange_name, routing_key='', body=body)
        
        # if is_sentinel:
        #     logging.info(f"FANOUT: Received sentinel! Shutting down...")
        #     raise KeyboardInterrupt

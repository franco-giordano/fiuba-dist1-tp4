import logging
from common.models.matches1v1_grouper import Matches1v1Grouper
from common.encoders.batch_encoder_decoder import BatchEncoderDecoder
from common.utils.rabbit_utils import RabbitUtils

class ShardedGrouperController:
    def __init__(self, rabbit_ip, shard_exchange_name, output_queue_name, assigned_shard_key):
        self.shard_exchange_name = shard_exchange_name
        self.assigned_shard_key = assigned_shard_key

        self.connection, self.channel = RabbitUtils.setup_connection_with_channel(rabbit_ip)

        # setup input exchange
        RabbitUtils.setup_input_direct_exchange(self.channel, self.shard_exchange_name, assigned_shard_key, self._callback)

        # setup output queue
        RabbitUtils.setup_queue(self.channel, output_queue_name)

        self.matches_grouper = Matches1v1Grouper(self.channel, output_queue_name)

    def run(self):
        logging.info(f'Q2 GROUPER {self.assigned_shard_key}: Waiting for messages. To exit press CTRL+C')
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logging.warning('Q2 GROUPER: ######### Received Ctrl+C! Stopping...')
            self.channel.stop_consuming()
        self.connection.close()

    def _callback(self, ch, method, properties, body):
        if BatchEncoderDecoder.is_encoded_sentinel(body):
            logging.info(f"Q2 GROUPER {self.assigned_shard_key}: Received sentinel! Flushing and shutting down...")
            self.matches_grouper.received_sentinel()
            raise KeyboardInterrupt

        batch = BatchEncoderDecoder.decode_bytes(body)
        logging.info(f'Q2 GROUPER {self.assigned_shard_key}: Received batch {body[:25]}...')

        for player in batch:
            self.matches_grouper.add_player_in_match(player)

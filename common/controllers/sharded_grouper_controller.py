import logging
from common.models.civilizations_grouper import CivilizationsGrouper
from common.encoders.batch_encoder_decoder import BatchEncoderDecoder
from common.utils.rabbit_utils import RabbitUtils
from common.models.sentinel_tracker import SentinelTracker

class ShardedGrouperController:
    def __init__(self, rabbit_ip, shard_exchange_name, output_queue_name, assigned_shard_key, aggregator, total_incoming_sentinels):
        self.shard_exchange_name = shard_exchange_name
        self.assigned_shard_key = assigned_shard_key

        self.connection, self.channel = RabbitUtils.setup_connection_with_channel(rabbit_ip)

        # setup input exchange
        RabbitUtils.setup_input_direct_exchange(self.channel, self.shard_exchange_name, assigned_shard_key, self._callback)

        # setup output queue
        RabbitUtils.setup_queue(self.channel, output_queue_name)

        self.civ_grouper = CivilizationsGrouper(self.channel, output_queue_name, aggregator)
        self.sentinel_tracker = SentinelTracker(total_incoming_sentinels)

    def run(self):
        logging.info(f'SHARDED GROUPER {self.assigned_shard_key}: Waiting for messages. To exit press CTRL+C')
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logging.warning('SHARDED GROUPER: ######### Received Ctrl+C! Stopping...')
            self.channel.stop_consuming()
        self.connection.close()

    def _callback(self, ch, method, properties, body):
        if BatchEncoderDecoder.is_encoded_sentinel(body):
            logging.info(f"SHARDED GROUPER {self.assigned_shard_key}: Received one sentinel!")
            if self.sentinel_tracker.count_and_reached_limit():
                logging.info(f"SHARDED GROUPER {self.assigned_shard_key}: Received all sentinels! Flushing and shutting down...")
                self.civ_grouper.received_sentinel()
                raise KeyboardInterrupt
            return

        joined_match = BatchEncoderDecoder.decode_bytes(body)
        logging.info(f'SHARDED GROUPER {self.assigned_shard_key}: Received joined match {body[:25]}...')

        self.civ_grouper.add_joined_match(joined_match)

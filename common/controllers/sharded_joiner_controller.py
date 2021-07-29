import logging

from common.encoders.batch_encoder_decoder import BatchEncoderDecoder
from common.models.matches_joiner import MatchesJoiner
from common.models.persisted_sentinel_tracker import PersistedSentinelTracker
from common.utils.rabbit_utils import RabbitUtils


class ShardedJoinerController:
    def __init__(self, rabbit_ip, shard_exchange_name, output_exchange_name, assigned_shard_key, next_reducers_amount,
                 total_incoming_sentinels, persistance_file, sentinels_file):
        self.shard_exchange_name = shard_exchange_name
        self.assigned_shard_key = assigned_shard_key

        self.connection, self.channel = RabbitUtils.setup_connection_with_channel(rabbit_ip)

        # setup input exchange
        RabbitUtils.setup_input_direct_exchange(self.channel, self.shard_exchange_name,
                                                assigned_shard_key, self._callback, queue_name=None, auto_ack=False)

        # setup output exchange
        RabbitUtils.setup_output_direct_exchange(self.channel, output_exchange_name)

        self.matches_joiner = MatchesJoiner(assigned_shard_key, self.channel, output_exchange_name,
                                            next_reducers_amount, persistance_file)
        # self.sentinel_tracker = SentinelTracker(total_incoming_sentinels)
        self.sentinel_tracker = PersistedSentinelTracker(total_incoming_sentinels, sentinels_file)

    def run(self):
        logging.info(f'SHARDED JOINER {self.assigned_shard_key}: Waiting for messages. To exit press CTRL+C')
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logging.warning(f'SHARDED JOINER {self.assigned_shard_key}: ######### Received Ctrl+C! Stopping...')
            self.channel.stop_consuming()
        self.connection.close()

    def _callback(self, ch, method, properties, body):
        if BatchEncoderDecoder.is_encoded_sentinel(body):
            logging.info(f"SHARDED JOINER {self.assigned_shard_key}: Received one sentinel!")
            if self.sentinel_tracker.count_and_reached_limit():
                logging.info(
                    f"SHARDED JOINER {self.assigned_shard_key}: Received all sentinels! Flushing and shutting down...")
                self.matches_joiner.received_sentinel()
                self.sentinel_tracker.reset()

            RabbitUtils.ack_from_method(self.channel, method)
            return

        batch = BatchEncoderDecoder.decode_bytes(body)
        # logging.info(f'SHARDED JOINER {self.assigned_shard_key}: Received batch {body[:25]}...')

        if BatchEncoderDecoder.is_players_batch(batch):
            self.matches_joiner.add_players_batch(batch)
            # logging.info(f'SHARDED JOINER {self.assigned_shard_key}: Added players batch {body[:25]}...')
        elif BatchEncoderDecoder.is_matches_batch(batch):
            self.matches_joiner.add_matches_batch(batch)
            # logging.info(f'SHARDED JOINER {self.assigned_shard_key}: Added matches batch {body[:25]}...')

        RabbitUtils.ack_from_method(self.channel, method)

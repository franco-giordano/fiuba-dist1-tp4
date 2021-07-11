from common.encoders.batch_encoder_decoder import BatchEncoderDecoder
from src.filter_pro_players import FilterProPlayers
from common.models.sharded_outgoing_batcher import ShardedOutgoingBatcher
from common.utils.rabbit_utils import RabbitUtils
import logging

class ProPlayersController:
    def __init__(self, rabbit_ip, players_exchange_name, output_exchange_name, reducers_amount, batch_size):
        self.players_exchange_name = players_exchange_name
        self.output_exchange_name = output_exchange_name

        self.connection, self.channel = RabbitUtils.setup_connection_with_channel(rabbit_ip)

        # input exchange
        RabbitUtils.setup_input_fanout_exchange(self.channel, self.players_exchange_name, self._callback)

        # output exchange
        RabbitUtils.setup_output_direct_exchange(self.channel, self.output_exchange_name)

        self.filter = FilterProPlayers()
        self.sharded_outgoing_batcher = ShardedOutgoingBatcher(self.channel, reducers_amount, batch_size, output_exchange_name, tkn_key='match')

    def run(self):
        logging.info('FILTER PRO PLAYERS: Waiting for messages. To exit press CTRL+C')
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logging.warning('FILTER PRO PLAYERS: ######### Received Ctrl+C! Stopping...')
            self.channel.stop_consuming()
        self.connection.close()

    def _callback(self, ch, method, properties, body):
        if BatchEncoderDecoder.is_encoded_sentinel(body):
            logging.info(f"FILTER PRO PLAYERS: Received sentinel! Shutting down...")
            self.sharded_outgoing_batcher.received_sentinel()
            raise KeyboardInterrupt

        batch = BatchEncoderDecoder.decode_bytes(body)

        for player in batch:
            if self.filter.should_pass(player):
                self.sharded_outgoing_batcher.add_to_batch(player)

        self.sharded_outgoing_batcher.publish_if_full()

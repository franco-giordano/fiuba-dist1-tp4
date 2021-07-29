import logging
from multiprocessing import Process

from common.encoders.batch_encoder_decoder import BatchEncoderDecoder
from common.models.sharded_outgoing_batcher import ShardedOutgoingBatcher
from common.utils.heartbeat import HeartBeat
from common.utils.rabbit_utils import RabbitUtils


class ShardExchangeController:
    def __init__(self, node_name, rabbit_ip, players_exchange_name, output_exchange_name, reducers_amount, pongs_queue,
                 batch_size):
        self.players_exchange_name = players_exchange_name

        self.connection, self.channel = RabbitUtils.setup_connection_with_channel(rabbit_ip)
        self.node_name = node_name
        self.pongs_queue = pongs_queue

        # Seteo heartbeat para todos
        self.heartbeat_process = Process(target=self._heartbeat_init, args=(rabbit_ip,))

        # setup input exchange
        RabbitUtils.setup_input_fanout_exchange(self.channel, self.players_exchange_name, self._callback)

        # setup output exchange
        RabbitUtils.setup_output_direct_exchange(self.channel, output_exchange_name)

        self.sharded_outgoing_batcher = ShardedOutgoingBatcher(self.channel, reducers_amount, batch_size,
                                                               output_exchange_name)

    def run(self):
        self.heartbeat_process.start()
        logging.info('SHARD EXCHANGE: Waiting for messages. To exit press CTRL+C')
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logging.warning('SHARD EXCHANGE: ######### Received Ctrl+C! Stopping...')
            self.channel.stop_consuming()
        self.connection.close()

    def _callback(self, ch, method, properties, body):
        if BatchEncoderDecoder.is_encoded_sentinel(body):
            # logging.info(f"SHARD EXCHANGE: Received sentinel! Propagating and shutting down...")
            self.sharded_outgoing_batcher.received_sentinel()
            # raise KeyboardInterrupt
            return

        batch = BatchEncoderDecoder.decode_bytes(body)
        # logging.info(f"SHARD EXCHANGE: Received batch {body[:25]}...")

        for player in batch:
            self.sharded_outgoing_batcher.add_to_batch(player)

        self.sharded_outgoing_batcher.publish_if_full()

    def _heartbeat_init(self, rabbit_ip):
        heartbeat = HeartBeat(self.node_name, rabbit_ip, self.pongs_queue)
        heartbeat.run()
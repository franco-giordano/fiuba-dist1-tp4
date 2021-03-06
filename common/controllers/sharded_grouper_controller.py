import logging
from multiprocessing import Process

from common.encoders.batch_encoder_decoder import BatchEncoderDecoder
from common.models.civilizations_grouper import CivilizationsGrouper
from common.models.sentinel_tracker import SentinelTracker
from common.utils.heartbeat import HeartBeat
from common.utils.rabbit_utils import RabbitUtils


class ShardedGrouperController:
    def __init__(self, node_name, rabbit_ip, shard_exchange_name, output_queue_name, pongs_queue, assigned_shard_key, aggregator,
                 joiners_amount, persistance_filename):
        self.node_name = node_name
        self.pongs_queue = pongs_queue
        self.shard_exchange_name = shard_exchange_name
        self.assigned_shard_key = assigned_shard_key
        self.joiners_amount = joiners_amount

        # Seteo heartbeat para todos
        self.heartbeat_process = Process(target=self._heartbeat_init, args=(rabbit_ip,))

        self.connection, self.channel = RabbitUtils.setup_connection_with_channel(rabbit_ip)
        # setup input exchange
        RabbitUtils.setup_input_direct_exchange(self.channel, self.shard_exchange_name,
                                                assigned_shard_key, self._callback, queue_name=None, auto_ack=False)

        # setup output queue
        RabbitUtils.setup_queue(self.channel, output_queue_name)

        self.civ_grouper = CivilizationsGrouper(assigned_shard_key, self.channel, output_queue_name, aggregator,
                                                self.joiners_amount, persistance_filename)

    def run(self):
        self.heartbeat_process.start()

        logging.info(f'SHARDED GROUPER {self.assigned_shard_key}: Waiting for messages. To exit press CTRL+C')
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logging.warning('SHARDED GROUPER: ######### Received Ctrl+C! Stopping...')
            self.channel.stop_consuming()
        self.connection.close()

    def _callback(self, ch, method, properties, body):
        join_msg = BatchEncoderDecoder.decode_bytes(body)
        # logging.info(f'SHARDED GROUPER {self.assigned_shard_key}: Received joined match {body[:25]}...')

        self.civ_grouper.recv_msg(join_msg, properties)

        # ACK a cola de input
        RabbitUtils.ack_from_method(self.channel, method)

    def _heartbeat_init(self, rabbit_ip):
        heartbeat = HeartBeat(self.node_name, rabbit_ip, self.pongs_queue)
        heartbeat.run()

"""
[APPEND id_fila_1, CHECK, APPEND id_fila_2, CHECK, APPEND id_fila_2]

{???mongol??? : set(ids)} dict[mongol].add(id_fila_2)

Leo una fila OK
Proceso
WRITE APPEND fila
WRITE CHECK
ACK input OK

"""

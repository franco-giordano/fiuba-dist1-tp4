import logging

from common.models.matches1v1_grouper import Matches1v1Grouper
# from common.models.civilizations_grouper import CivilizationsGrouper
from common.utils.rabbit_utils import RabbitUtils


class ShardedGrouperController:
    def __init__(self, id_grouper, rabbit_ip, shard_exchange_name, output_queue_name, persistance_filename):
        self.id_grouper = id_grouper
        self.shard_exchange_name = shard_exchange_name

        self.connection, self.channel = RabbitUtils.setup_connection_with_channel(rabbit_ip)

        # setup input exchange
        RabbitUtils.setup_input_direct_exchange(self.channel, self.shard_exchange_name,
                                                id_grouper, self._callback, queue_name=None, auto_ack=False)

        # setup output queue
        RabbitUtils.setup_queue(self.channel, output_queue_name)

        self.matches_grouper = Matches1v1Grouper(self.id_grouper, self.channel, output_queue_name, persistance_filename)

    def run(self):
        logging.info(f'SHARDED GROUPER {self.id_grouper}: Waiting for messages. To exit press CTRL+C')
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logging.warning('SHARDED GROUPER: ######### Received Ctrl+C! Stopping...')
            self.channel.stop_consuming()
        self.connection.close()

    def _callback(self, ch, method, properties, body):
        # logging.info(f'SHARDED GROUPER {self.assigned_shard_key}: Received joined match {body[:25]}...')
        self.matches_grouper.recv_msg(body)
        # ACK a cola de input
        RabbitUtils.ack_from_method(self.channel, method)

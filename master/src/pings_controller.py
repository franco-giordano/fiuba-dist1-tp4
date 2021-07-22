from common.encoders.batch_encoder_decoder import BatchEncoderDecoder
from common.utils.rabbit_utils import RabbitUtils
import logging
from datetime import datetime
from threading import Timer
import subprocess


class PingsController:
    def __init__(self, rabbit_ip, pongs_queue, nodes_list):
        self.pongs_queue = pongs_queue
        # self.current_leader = -1
        self.NODES_TIMEOUT = 10

        self.connection, self.channel = RabbitUtils.setup_connection_with_channel(
            rabbit_ip)

        RabbitUtils.setup_input_queue(
            self.channel, pongs_queue, self._pongs_callback)

        self.nodes_status = {}

        for n in nodes_list:
            self.nodes_status[n] = Timer(
                self.NODES_TIMEOUT, self._time_up, args=(n,))

    def run(self):
        for n in self.nodes_status.keys():
            self.nodes_status[n].start()

        logging.info('PINGS: Waiting for messages. To exit press CTRL+C')
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logging.warning('PINGS: ######### Received Ctrl+C! Stopping...')
            self.channel.stop_consuming()
        self.connection.close()

    def _time_up(self, node_name):
        # node down
        self._restart_node(node_name)
        self._restart_timer(node_name)

    def _pongs_callback(self, ch, method, properties, body):
        node_name = PongEncoderDecoder.decode_bytes(body)
        self._restart_timer(node_name)

    def _restart_node(self, node_name):
        result = subprocess.run(['docker', 'stop', node_name],
                                check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logging.info(
            f'Stopped container {node_name}. Result={result.returncode}. Output={result.stdout}. Error={result.stderr}')

        result = subprocess.run(['docker', 'start', node_name],
                                check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logging.info(
            f'Started container {node_name}. Result={result.returncode}. Output={result.stdout}. Error={result.stderr}')

    def _restart_timer(self, node_name):
        self.nodes_status[node_name].cancel()
        self.nodes_status[node_name] = Timer(
            self.NODES_TIMEOUT, self._time_up, args=(node_name,))
        self.nodes_status[node_name].start()

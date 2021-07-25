from collections import defaultdict
from threading import Thread, Lock
from common.encoders.batch_encoder_decoder import BatchEncoderDecoder
from common.encoders.obj_encoder_decoder import ObjectEncoderDecoder
from common.utils.rabbit_utils import RabbitUtils
import logging
from datetime import date, datetime
import subprocess
from time import sleep


class PingsController:
    def __init__(self, rabbit_ip, pongs_queue, nodes_list):
        self.pongs_queue = pongs_queue
        self.timeout_interval = 5  # segundos

        self.connection, self.channel = RabbitUtils.setup_connection_with_channel(
            rabbit_ip)

        RabbitUtils.setup_input_queue(
            self.channel, pongs_queue, self._pongs_callback)

        self.generations = [{}, {}]
        self.act_generation = 0  # Para cambiarla hago algo como ^=1
        self.generations_lock = Lock()

        self.healthcheck_thread = Thread(target=self.nodes_healthcheck)

    def run(self):

        self.healthcheck_thread.start()

        logging.info('PINGS: Waiting for messages. To exit press CTRL+C')
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logging.warning('PINGS: ######### Received Ctrl+C! Stopping...')
            self.channel.stop_consuming()
        finally:
            self.healthcheck_thread.join()
            self.connection.close()

    def nodes_healthcheck(self):
        """
        Ciclo por el diccionario viejo y hago el chequeo si queda alguien.
        Levanto todo lo que haya quedado en el old_generation (self.act_generation ^= 1).

        """
        while True:
            with self.generations_lock:
                nodes_to_restart = self.generations[self.act_generation ^ 1].keys()
                
                for node_name in nodes_to_restart:
                    self._restart_node(node_name)
                    
                    del self.generations[self.act_generation ^ 1][node_name]
                    self.generations[self.act_generation][node_name] = datetime.now() # Lo levanto y le asigno el timestamp actual


                self.act_generation ^= 1 # Paso todo lo de la generación nueva a vieja
            
            sleep(self.timeout_interval)
            

    def _time_up(self, node_name):
        # node down
        self._restart_node(node_name)
        self._restart_timer(node_name)

    def _pongs_callback(self, ch, method, properties, body):
        node_name = ObjectEncoderDecoder.decode_bytes(body)
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
        with self.generations_lock:
            self.generations[self.act_generation][node_name] = datetime.now()

            if node_name in self.generations[self.act_generation ^ 1]: # Borro de la vieja generación
                del self.generations[self.generations ^ 1][node_name]

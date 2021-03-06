import logging
import subprocess
from datetime import datetime
from threading import Thread, Lock
from time import sleep

from common.encoders.obj_encoder_decoder import ObjectEncoderDecoder
from common.models.persistor import Persistor
from common.utils.rabbit_utils import RabbitUtils


class PingsController:
    def __init__(self, rabbit_ip, my_master_id, pongs_queue, nodes_list, log_filename):
        self.my_master_id = my_master_id
        self.pongs_queue = pongs_queue
        self.timeout_interval = 10  # segundos
        self.nodes_list = nodes_list
        self.log_filename = log_filename
        self.log_persistor = Persistor(self.log_filename)

        self.connection, self.channel = RabbitUtils.setup_connection_with_channel(
            rabbit_ip)

        RabbitUtils.setup_input_queue(
            self.channel, pongs_queue, self._pongs_callback)

        self.generations = [{}, {}]
        self.act_generation = 0  # Para cambiarla hago algo como ^=1
        self.generations_lock = Lock()

        self.healthcheck_thread = Thread(target=self.nodes_healthcheck)
        self.keep_going = True

    def run(self):
        self._gens_init()
        self.healthcheck_thread.start()

        logging.info('PINGS: Waiting for messages. To exit press CTRL+C')
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logging.warning('PINGS: ######### Received Ctrl+C! Stopping...')
            self.channel.stop_consuming()
        finally:
            self.keep_going = False
            self.healthcheck_thread.join()
            self.connection.close()

    # runs threaded
    def nodes_healthcheck(self):
        """
        Ciclo por el diccionario viejo y hago el chequeo si queda alguien.
        Levanto todo lo que haya quedado en el old_generation (self.act_generation ^= 1).

        """
        while self.keep_going:
            with self.generations_lock:
                nodes_to_restart = list(
                    self.generations[self.act_generation ^ 1].keys())

                for node_name in nodes_to_restart:
                    self._restart_node(node_name)

                    del self.generations[self.act_generation ^ 1][node_name]
                    # Lo levanto y le asigno el timestamp actual
                    self.generations[self.act_generation][node_name] = datetime.now()

                self.act_generation ^= 1  # Paso todo lo de la generaci??n nueva a vieja

            sleep(self.timeout_interval)

    def _pongs_callback(self, ch, method, properties, body):  # callback para la cola del pings controller
        node_name = ObjectEncoderDecoder.decode_bytes(body).strip()
        # logging.info(f'PINGS: Recibido {body}')
        if node_name != f"master-{self.my_master_id}":
            self._restart_timer(node_name)

    def _restart_node(self, node_name):
        result = subprocess.run(['docker', 'stop', node_name],
                                check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logging.info(
            f'Stopped container {node_name}. Result={result.returncode}. Output={result.stdout}. Error={result.stderr}')

        self.log_persistor.log(
            f'Restarting container {node_name}. Result={result.returncode}. Output={result.stdout}. Error={result.stderr}')

        result = subprocess.run(['docker', 'start', node_name],
                                check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logging.info(
            f'Started container {node_name}. Result={result.returncode}. Output={result.stdout}. Error={result.stderr}')

    def _restart_timer(self, node_name):
        with self.generations_lock:
            self.generations[self.act_generation][node_name] = datetime.now()

            # Borro de la vieja generaci??n
            if node_name in self.generations[self.act_generation ^ 1]:
                del self.generations[self.act_generation ^ 1][node_name]

    def _gens_init(self):
        if self.nodes_list == ['']:  # empty nodes list
            return

        for n in self.nodes_list:
            if n != f"master-{self.my_master_id}":
                self.generations[self.act_generation][n] = datetime.now()

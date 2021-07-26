from common.utils.heartbeat import HeartBeat
import os
import signal
import logging
from common.utils.master_utils import MasterUtils
from src.pings_controller import PingsController
from common.encoders.obj_encoder_decoder import ObjectEncoderDecoder
from multiprocessing import Process
from datetime import datetime
from time import sleep
from threading import Thread, Lock

TIMEOUT_ELECTION = TIMEOUT_COORDINATOR = 5


class MasterController:
    def __init__(self, rabbit_ip, master_comms_exchange, my_master_id, masters_amount,
                 pongs_queue, node_list, log_filename=None):
        self.master_comms_exchange = master_comms_exchange
        self.my_master_id = my_master_id
        self.masters_amount = masters_amount
        self.current_leader = -1
        self.log_filename = log_filename
        self.pongs_queue = pongs_queue
        self.node_list = node_list

        self.election_timer = None  # None: No hay elección transurriendo
        self.election_timer_lock = Lock()
        self.election_timer_check_thread = Thread(target=self.election_timer_check)
        
        self.coordinator_timer = None  # None: No hay elección transurriendo
        self.coordinator_timer_lock = Lock()
        self.coordinator_timer_check_thread = Thread(target=self.coordinator_timer_check)

        self.connection, self.channel = MasterUtils.setup_connection_with_channel(
            rabbit_ip)

        # Seteo heartbeat para todos
        heartbeat = HeartBeat(
            f"master-{self.my_master_id}", self.rabbit_ip, self.pongs_queue)
        self.heartbeat_process = Process(target=heartbeat.run)

        # comms exchange with other masters
        MasterUtils.setup_master_comms(
            self.channel, master_comms_exchange, my_master_id, self._comms_callback)

    def run(self):
        self.heartbeat_process.start()
        self.election_timer_check_thread.start()
        self.coordinator_timer_check_thread.start()

        self.celebrate_election()

        logging.info('MASTER: Waiting for messages. To exit press CTRL+C')
        try:
            # para testear mando un msg incial
            MasterUtils.send_to_all_masters(
                self.channel,
                self.master_comms_exchange,
                self.my_master_id,
                f'primer hola desde {self.my_master_id}',
                self.masters_amount)

            self.channel.start_consuming()
        except KeyboardInterrupt:
            logging.warning('MASTER: ######### Received Ctrl+C! Stopping...')
            self.channel.stop_consuming()
        finally:
            self.election_timer_check_thread.join()
            self.heartbeat_process.join()
            self.connection.close()

    def celebrate_election(self):
        self.current_leader = -1

        if self.my_master_id < self.master_amount - 1:  # No soy el mas grande, llamo a una eleccion
            election_msg = ObjectEncoderDecoder.encode_obj(
                {"type": "[[ELECTION]]", "id": self.my_master_id})
            MasterUtils.send_to_greater_ids(self.channel, self.master_comms_exchange, self.my_master_id,
                                            election_msg, self.masters_amount)

        else:  # Ya soy el mas grande, me apropio del liderazgo
            coordinator_msg = ObjectEncoderDecoder.encode_obj(
                {"type": "[[COORDINATOR]]", "id": self.my_master_id})
            MasterUtils.send_to_greater_ids(self.channel, self.master_comms_exchange, self.my_master_id,
                                            coordinator_msg, self.masters_amount)

        self.channel.start_consuming()

    def election_timer_check(self):
        while True:
            with self.election_timer_lock:
                if self.election_timer is not None:
                    elapsed_seconds = (datetime.now() - self.election_timer).seconds
                    if elapsed_seconds > TIMEOUT_ELECTION:
                        coordinator_msg = ObjectEncoderDecoder.encode_obj(
                            {"type": "[[COORDINATOR]]", "id": self.my_master_id})
                        MasterUtils.send_to_all_masters(self.channel, self.master_comms_exchange,
                                                        self.my_master_id, coordinator_msg, self.master_amount)

            sleep(TIMEOUT_ELECTION/2)  

    def coordinator_timer_check(self):
        while True:
            with self.coordinator_timer_lock:
                if self.coordinator_timer is not None:
                    elapsed_seconds = (datetime.now() - self.coordinator_timer).seconds
                    if elapsed_seconds > TIMEOUT_COORDINATOR:
                        election_msg = ObjectEncoderDecoder.encode_obj(
                            {"type": "[[ELECTION]]", "id": self.my_master_id})
                        MasterUtils.send_to_greater_ids(self.channel, self.master_comms_exchange,
                                                        self.my_master_id, election_msg, self.master_amount)

            sleep(TIMEOUT_COORDINATOR/2)  # 2 segundos

    def _comms_callback(self, ch, method, properties, body):
        event = ObjectEncoderDecoder.decode_bytes(body)

        # IF ELECTION: Respondo OK, forwardeo a nodos mas grandes, sigo escuchando
        if event["type"] == "[[ELECTION]]":
            if self.my_master_id == self.current_leader:
                # No voy a estar monitoreando heartbeats durante la elección
                os.kill(self.pings_process.pid, signal.SIGINT)

            MasterUtils.send_alive_bully_msg(
                self.channel, self.master_comms_exchange, self.my_master_id, event["id"])

            election_msg = ObjectEncoderDecoder.encode_obj(
                {"type": "[[ELECTION]]"})
            MasterUtils.send_to_greater_ids(self.channel, self.master_comms_exchange, self.my_master_id,
                                            election_msg, self.masters_amount)

            with self.election_timer_lock:
                self.election_timer = datetime.now()

        # IF ALIVE: Sigo consumiendo, hasta esperar el mensaje coordinator, ya no voy a ser el lider
        if event["type"] == "[[ALIVE]]":
            with self.election_timer_lock:
                self.election_timer = None

            with self.coordinator_timer_lock:
                self.coordinator_time = datetime.now()
            # TODO: Setear nuevo timer a la espera de coordinator (por si justo se cae quien gano la eleccion antes de mandarlo)
            # Si se vence el timer, arranco una elección de nuevo

        # IF COORDINATOR-{id}: Actualizo coordinator y empiezo a mandar heartbeats (por la de pongs, en otro proceso)
        #  y empiezo a monitorear al lider
        if event["type"] == "[[COORDINATOR]]":
            new_leader = event["id"]
            self.current_leader = new_leader

            with self.coordinator_timer_lock:
                self.coordinator_timer = None

        # IF LIDER VIVO
        #  reseteo el timer del lider

        logging.info(f"MASTER-{self.my_master_id}: Received msg '{body}'")

    def _time_up(self, ):
        """ Se invoca cuando envio [[ELECTION]] y no recibo respuesta, significa que soy el coordinator """

        # Se ejecutara esto si el timer vence
        pings_process = Process(target=self.pings_init)
        pings_process.start()

        MasterUtils.send_to_all_masters(self.channel, self.master_comms_exchange, self.my_master_id,
                                        ObjectEncoderDecoder.encode_obj(
                                            {"type": "[[COORDINATOR]]", "id": self.my_master_id}),
                                        self.masters_amount)

    def pings_init(self):
        controller = PingsController(
            self.my_master_id, self.rabbit_ip, self.pongs_queue, self.nodes_list, self.log_filename)
        controller.run()

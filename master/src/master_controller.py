import logging
import os
import signal
from datetime import datetime
from multiprocessing import Process
from threading import Thread, Lock
from time import sleep

from src.internal_monitor import InternalMonitor
from src.pings_controller import PingsController

from common.encoders.obj_encoder_decoder import ObjectEncoderDecoder
from common.utils.heartbeat import HeartBeat
from common.utils.master_utils import MasterUtils

TIMEOUT_ELECTION = 4
TIMEOUT_COORDINATOR = 12


class MasterController:
    def __init__(self, rabbit_ip, master_comms_exchange, my_master_id, masters_amount,
                 pongs_queue, nodes_list, log_filename):
        self.rabbit_ip = rabbit_ip
        self.master_comms_exchange = master_comms_exchange
        self.my_master_id = my_master_id
        self.masters_amount = masters_amount
        self.current_leader = None
        self.log_filename = log_filename
        self.pongs_queue = pongs_queue
        self.nodes_list = nodes_list

        self.election_timer = None  # None: No hay elección transurriendo
        self.election_timer_lock = Lock()
        self.election_timer_check_thread = Thread(target=self.election_timer_check)

        # None: No estoy esperando un msj [[CCORD]]
        self.coordinator_timer = None
        self.coordinator_timer_lock = Lock()
        self.coordinator_timer_check_thread = Thread(target=self.coordinator_timer_check)

        self.connection, self.channel = MasterUtils.setup_connection_with_channel(rabbit_ip)

        self.internal_monitor = InternalMonitor(rabbit_ip, self.celebrate_election, self.my_master_id,
                                                self.master_comms_exchange, self.masters_amount)

        # Seteo heartbeat para todos
        self.heartbeat_process = Process(target=self._heartbeat_init, args=(rabbit_ip,))

        # comms exchange with other masters
        MasterUtils.setup_master_comms(self.channel, master_comms_exchange, my_master_id, self._comms_callback)

        self.pings_process = None

    def run(self):
        self.heartbeat_process.start()
        self.election_timer_check_thread.start()
        self.coordinator_timer_check_thread.start()

        self.celebrate_election()

        logging.info('MASTER: Waiting for messages. To exit press CTRL+C')
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logging.info('MASTER: ######### Received Ctrl+C! Stopping...')
            self.channel.stop_consuming()
        finally:
            with self.election_timer_lock:
                self.election_timer = None
            with self.coordinator_timer_lock:
                self.coordinator_timer = None

            self.internal_monitor.stop_monitoring()
            self.election_timer_check_thread.join()
            self.heartbeat_process.join()
            self.connection.close()

    def celebrate_election(self):
        self.current_leader = None

        if self.my_master_id < self.masters_amount - 1:  # No soy el mas grande, llamo a una eleccion
            self._forward_election()
        else:  # Ya soy el mas grande, me apropio del liderazgo
            self._declare_my_leadership()

    def election_timer_check(self):
        """ Timer que regula si no recibo ALIVEs luego de iniciar una ELECTION """
        while True:
            with self.election_timer_lock:
                if self.election_timer is not None:
                    elapsed_seconds = (
                            datetime.now() - self.election_timer).seconds
                    if elapsed_seconds > TIMEOUT_ELECTION:
                        logging.info(f'MASTER: Salto election_timer! Declarandome como nuevo lider')
                        self._election_time_up()
                        self.election_timer = None  # turn off timer

            sleep(TIMEOUT_ELECTION / 2)

    def coordinator_timer_check(self):
        while True:
            with self.coordinator_timer_lock:
                if self.coordinator_timer is not None:
                    elapsed_seconds = (
                            datetime.now() - self.coordinator_timer).seconds
                    if elapsed_seconds > TIMEOUT_COORDINATOR:
                        logging.info(f'MASTER: Salto coordinator_timer! Iniciando una nueva eleccion')
                        self._forward_election()
                        self.coordinator_timer = None  # turn off timer

            sleep(TIMEOUT_COORDINATOR / 2)  # 2 segundos

    def _comms_callback(self, ch, method, properties, body):
        event = ObjectEncoderDecoder.decode_bytes(body)
        logging.info(f"MASTER-{self.my_master_id}: Received msg '{event}'")

        # IF ELECTION: Respondo OK, forwardeo a nodos mas grandes, sigo escuchando
        if event["type"] == "[[ELECTION]]":
            assert (event["id"] < self.my_master_id)
            MasterUtils.send_alive_bully_msg(
                self.channel, self.master_comms_exchange, self.my_master_id, event["id"])

            if self.am_i_leader():
                # os.kill(self.pings_process.pid, signal.SIGINT) OBSOLETO (creo)
                # decirle que yo soy el lider, que no flashe confianza
                MasterUtils.send_one_coordinator_msg(self.channel, self.master_comms_exchange, self.my_master_id,
                                                     event["id"])
                return
            else:  # soy slave, sigo buscando mi lider uwu
                self._forward_election()

        # IF ALIVE: Sigo consumiendo, hasta esperar el mensaje coordinator, ya no voy a ser el lider
        if event["type"] == "[[ALIVE]]":
            logging.info(f"MASTER: recibo [[ALIVE]] desde {event['id']} siendo {self.my_master_id}")
            if self.am_i_leader():
                assert (event["id"] < self.my_master_id)
                # No voy a estar monitoreando heartbeats durante la elección
                os.kill(self.pings_process.pid, signal.SIGINT)
            with self.election_timer_lock:
                self.election_timer = None

            with self.coordinator_timer_lock:
                self.coordinator_timer = datetime.now()

        # IF COORDINATOR-{id}: Actualizo coordinator y empiezo a mandar heartbeats (por la de pongs, en otro proceso)
        #  y empiezo a monitorear al lider
        if event["type"] == "[[COORDINATOR]]":
            new_leader = event["id"]
            assert (new_leader > self.my_master_id)
            if self.am_i_leader():
                os.kill(self.pings_process.pid, signal.SIGINT)

            self.current_leader = new_leader

            with self.election_timer_lock:
                self.election_timer = None

            with self.coordinator_timer_lock:
                self.coordinator_timer = None
            self.internal_monitor.start_monitoring_leader()

        """
        LIDER:
        - mande heartbeats a los slaves por el exchange de mastercomms
            en nuevo thread
        SLAVES:
        - monitorear heartbeats que vengan por mastercomms, sino llamar eleccion
            con timers, no hace falta thread
        """
        # IF LIDER VIVO
        if event["type"] == "[[LEADER_ALIVE]]":
            assert (event["id"] == self.current_leader)

            #  reseteo el timer del lider
            self.internal_monitor.reset_leader_timer()

    def _election_time_up(self):
        """ Se invoca cuando envio [[ELECTION]] y no recibo respuesta, significa que soy el coordinator """
        # Se ejecutara esto si el timer vence
        self._declare_my_leadership()

    def pings_init(self):
        controller = PingsController(self.rabbit_ip, self.my_master_id, self.pongs_queue, self.nodes_list,
                                     self.log_filename)
        controller.run()

    def _forward_election(self):
        logging.info('MASTER: iniciando [[ELECTION]] !')

        with self.election_timer_lock:
            # espero que alguien me avise que es lider (ya me llego un ALIVE)
            self.election_timer = datetime.now()

        conn, channel = MasterUtils.setup_connection_with_channel(self.rabbit_ip)
        election_msg = ObjectEncoderDecoder.encode_obj(
            {"type": "[[ELECTION]]", "id": self.my_master_id})
        MasterUtils.send_to_greater_ids(channel, self.master_comms_exchange,
                                        self.my_master_id, election_msg, self.masters_amount)

    def _declare_my_leadership(self):
        logging.info('MASTER: declarandome como [[COORDINATOR]] (⌐■_■)')
        self.internal_monitor.start_sending_heartbeats()

        self.current_leader = self.my_master_id

        conn, channel = MasterUtils.setup_connection_with_channel(self.rabbit_ip)
        coord_msg = ObjectEncoderDecoder.encode_obj({"type": "[[COORDINATOR]]", "id": self.my_master_id})

        MasterUtils.send_to_all_masters(channel, self.master_comms_exchange,
                                        self.my_master_id, coord_msg, self.masters_amount)

        logging.warning(f'MASTER: PROCESO PINGSCNTRLR: {self.pings_process if self.pings_process else None}')
        if self.pings_process and self.pings_process.is_alive():
            logging.error(f'MASTER: TRATANDO DE LEVANTAR DOS VECES PINGSCONTROLLER!!!!!')
        self.pings_process = Process(target=self.pings_init)
        self.pings_process.start()

    def _heartbeat_init(self, rabbit_ip):
        heartbeat = HeartBeat(
            f"master-{self.my_master_id}", rabbit_ip, self.pongs_queue)
        heartbeat.run()

    def am_i_leader(self):
        return self.my_master_id == self.current_leader

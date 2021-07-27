from threading import Thread, Lock
from common.utils.master_utils import MasterUtils
from common.encoders.obj_encoder_decoder import ObjectEncoderDecoder
from time import sleep
import logging
from datetime import datetime

TIMEOUT_LEADER = 5

# pingscontroller + heartbeat.py pero para monitoreo de lider, no de nodos


class InternalMonitor:
    def __init__(self, rabbit_ip, leader_dead_callback,
                 my_master_id, master_comms_exchange, masters_amount):

        self.heartbeat_sender_thread = None # Thread(target=self._heartbeat_loop)
        self.leader_monitor_thread = None # Thread(
        #     target=self._leader_monitor, args=(leader_dead_callback,))
        self.rabbit_ip = rabbit_ip

        self.leader_timer = None
        self.leader_timer_lock = Lock()

        self.is_leader = False
        self.is_leader_lock = Lock()

        self.leader_dead_callback = leader_dead_callback
        self.my_master_id = my_master_id
        self.master_comms_exchange = master_comms_exchange
        self.masters_amount = masters_amount
        self.keep_going = True

    def start_sending_heartbeats(self):
        logging.info('INTERNAL_MON: Ahora soy lider (⌐■_■) enviando heartbeats')
        self._set_is_leader(True)
        if self.leader_monitor_thread:
            self.leader_monitor_thread.join()

        if self.heartbeat_sender_thread and self.heartbeat_sender_thread.is_alive():
            logging.error('INTERNAL_MON: TRATANDO DE LEVANTAR DOS HEARTBEAT_SENDER!')
            return

        self.heartbeat_sender_thread = Thread(target=self._heartbeat_loop)
        self.heartbeat_sender_thread.start()

    def start_monitoring_leader(self):
        logging.info('INTERNAL_MON: Ahora soy slave, monitoreando a lider')
        self._set_is_leader(False)
        self.leader_timer = datetime.now()

        if self.heartbeat_sender_thread:
            self.heartbeat_sender_thread.join()

        if self.leader_monitor_thread and self.leader_monitor_thread.is_alive():
            logging.error('INTERNAL_MON: TRATANDO DE LEVANTAR DOS LEADER_MONITOR!')
            return

        self.leader_monitor_thread = Thread(
            target=self._leader_monitor, args=(self.leader_dead_callback,))
        self.leader_monitor_thread.start()

    def reset_leader_timer(self):
        self.leader_timer = datetime.now()

    def _leader_monitor(self, callback):

        while self.keep_going:
            with self.leader_timer_lock:
                if self.leader_timer is not None:
                    elapsed_seconds = (datetime.now() - self.leader_timer).seconds
                    if elapsed_seconds > TIMEOUT_LEADER:
                        # se cae el lider !
                        logging.info('INTERNAL_MON: Murio el lider! Llamando callback')
                        self.leader_timer = None
                        callback()

            sleep(2)  # segundos

            with self.is_leader_lock:
                if self.is_leader:  # si ahora soy lider, debo dejar de monitorear
                    with self.leader_timer_lock:
                        self.leader_timer = None
                    break

    def _heartbeat_loop(self):
        conn, channel =  MasterUtils.setup_connection_with_channel(self.rabbit_ip)

        heartbeat_body = ObjectEncoderDecoder.encode_obj(
            {"type": "[[LEADER_ALIVE]]", "id": self.my_master_id})
        while self.keep_going:
            MasterUtils.send_to_all_masters(channel, self.master_comms_exchange,
                                        self.my_master_id, heartbeat_body, self.masters_amount)

            sleep(1.5)

            with self.is_leader_lock:
                if not self.is_leader:  # si ya no soy lider, debo dejar de heartbeatear
                    break

    def _set_is_leader(self, value):
        with self.is_leader_lock:
            self.is_leader = value

    def stop_monitoring(self):
        # desactivo si estoy monitoreando leader
        with self.leader_timer_lock:
            self.leader_timer = None

        # desactivo si estoy mandando heartbeats
        self._set_is_leader(False)

        # desactivo todo
        self.keep_going = False

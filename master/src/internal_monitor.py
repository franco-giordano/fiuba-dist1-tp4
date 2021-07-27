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

        self.connection, self.channel = MasterUtils.setup_connection_with_channel(
            rabbit_ip)
        self.heartbeat_sender_thread = None # Thread(target=self._heartbeat_loop)
        self.leader_monitor_thread = None # Thread(
        #     target=self._leader_monitor, args=(leader_dead_callback,))

        self.leader_timer = None
        self.leader_timer_lock = Lock()

        self.is_leader = False
        self.is_leader_lock = Lock()

        self.leader_dead_callback = leader_dead_callback
        self.my_master_id = my_master_id
        self.master_comms_exchange = master_comms_exchange
        self.masters_amount = masters_amount

    def start_sending_heartbeats(self):
        logging.info(
            'INTERNAL_MON: Ahora soy lider (⌐■_■) enviando heartbeats')
        self._set_is_leader(True)
        if self.leader_monitor_thread:
            self.leader_monitor_thread.join()
        self.heartbeat_sender_thread = Thread(target=self._heartbeat_loop)
        self.heartbeat_sender_thread.start()

    def start_monitoring_leader(self):
        logging.info('INTERNAL_MON: Ahora soy slave, monitoreando a lider')
        self._set_is_leader(False)
        self.leader_timer = datetime.now()

        if self.heartbeat_sender_thread:
            self.heartbeat_sender_thread.join()

        self.leader_monitor_thread = Thread(
            target=self._leader_monitor, args=(self.leader_dead_callback,))
        self.leader_monitor_thread.start()

    def reset_leader_timer(self):
        self.leader_timer = datetime.now()

    def _leader_monitor(self, callback):
        while True:
            with self.leader_timer_lock:
                if self.leader_timer is not None:
                    elapsed_seconds = (
                        datetime.now() - self.leader_timer).seconds
                    if elapsed_seconds > TIMEOUT_LEADER:
                        # se cae el lider !
                        logging.info(
                            'INTERNAL_MON: Murio el lider! Llamando callback')
                        callback()

            sleep(2.5)  # segundos

            with self.is_leader_lock:
                if self.is_leader:  # si ahora soy lider, debo dejar de monitorear
                    with self.leader_timer_lock:
                        self.leader_timer = None
                    break

    def _heartbeat_loop(self):
        heartbeat_body = ObjectEncoderDecoder.encode_obj(
            {"type": "[[LEADER_ALIVE]]", "id": self.my_master_id})
        while True:
            MasterUtils.send_to_all_masters(self.channel, self.master_comms_exchange,
                                        self.my_master_id, heartbeat_body, self.masters_amount)

            sleep(2.5)

            with self.is_leader_lock:
                if not self.is_leader:  # si ya no soy lider, debo dejar de heartbeatear
                    break

    def _set_is_leader(self, value):
        with self.is_leader_lock:
            self.is_leader = value

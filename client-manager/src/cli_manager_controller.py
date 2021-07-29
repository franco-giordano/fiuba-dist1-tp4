import logging

from common.encoders.api_pkts_encoder_decoder import ApiPacketsEncoder
from common.models.persistor import Persistor
from common.utils.heartbeat import HeartBeat
from common.utils.rabbit_utils import RabbitUtils
from multiprocessing import Process


# Protocolo de cli manager
# Desencolar una consulta con id_cliente
# Chequear en memoria si está disponible el sistema (el master o alguien debería avisarle al
# cli manager que esta listo el sistema)
# Persistir que ahora le corresponde el sistema a id_cliente1 (o no)
#     escribo para liberar el sistema (LIBR) -> Libre
#     escribo si el sistema esta libre y lo quiero ocupar (id_cli3 che) -> Libre

# Mandar respuesta al cliente
# ACK al mensaje de request cliente - si el cliente recibe algún reply inesperado, chequea el correlation id :) .
# (Usar reply_to para no tener una unica cola de vuelta)

class CliManagerController:
    def __init__(self, rabbit_ip, requests_queue_name, pongs_queue, sys_status_filename):
        self.requests_queue_name = requests_queue_name

        self.NOTIFY_SYS_IDLE_MSG = "NOTIFY_SYS_IDLE"

        self.persistor_state = Persistor(sys_status_filename)
        self.system_state = self.reload_persisted_state()  # (container_name | 'FREE')
        self.pongs_queue = pongs_queue

        # Seteo heartbeat para todos
        self.heartbeat_process = Process(target=self._heartbeat_init, args=(rabbit_ip,))

        self.connection, self.channel = RabbitUtils.setup_connection_with_channel(rabbit_ip)
        # setup input queue
        RabbitUtils.setup_input_queue(self.channel, self.requests_queue_name, self._callback, auto_ack=False)

    def run(self):
        self.heartbeat_process.start()

        logging.info('CLIMANAGER: Waiting for messages. To exit press CTRL+C')
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logging.warning('CLIMANAGER: ######### Received Ctrl+C! Stopping...')
            self.channel.stop_consuming()
        self.connection.close()

    def _callback(self, ch, method, props, body):  # La defino aca adentro para poder usar el self.
        # Parsear y chequear el input
        request = ApiPacketsEncoder.decode_bytes(body)
        request_client_id, request_msg = request['id'], request['msg']

        if request_msg == self.NOTIFY_SYS_IDLE_MSG:
            self._release_sys(method)
            return

        reply_msg = ApiPacketsEncoder.create_sys_busy()
        logging.info(f"Estado del sistema: {self.system_state}")

        if self.system_state == request_client_id or self.system_state == 'FREE':
            reply_msg = ApiPacketsEncoder.create_ok_to_upload()
            self.system_state = request_client_id
            self.persistor_state.update(request_client_id)

        RabbitUtils.send_to_queue(ch, props.reply_to, reply_msg, corr_id=props.correlation_id)
        RabbitUtils.ack_from_method(self.channel, method)

    def _release_sys(self, method):
        logging.info("El sistema nuevamente se encuentra disponible")
        self.system_state = 'FREE'
        self.persistor_state.update('FREE')

        RabbitUtils.ack_from_method(self.channel, method)

    def reload_persisted_state(self):
        persisted_state = self.persistor_state.read()

        if not persisted_state:  # Si el archivo esta corrupto, el sistema en cualquier caso debería update a libre
            self.persistor_state.update("FREE")
            return "FREE"
        else:
            # assert(len(persisted_state) == 2)
            return persisted_state[0].strip()

    def _heartbeat_init(self, rabbit_ip):
        heartbeat = HeartBeat("client-manager", rabbit_ip, self.pongs_queue)
        heartbeat.run()

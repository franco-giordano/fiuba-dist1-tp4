from common.utils.rabbit_utils import RabbitUtils
from common.encoders.api_pkts_encoder_decoder import ApiPacketsEncoder
import logging
from common.models.persistor import Persistor

# Protocolo de cli manager
# Desencolar una consulta con id_cliente
# Chequear en memoria si está disponible el sistema (el master o alguien debería avisarle al cli manager que esta listo el sistema)
# Persistir que ahora le corresponde el sistema a id_cliente1 (o no)
#     escribo para liberar el sistema (LIBR) -> Libre
#     escribo si el sistema esta libre y lo quiero ocupar (id_cli3 che) -> Libre

# Mandar respuesta al cliente
# ACK al mensaje de request cliente - si el cliente recibe algún reply inesperado, chequea el correlation id :) . (Usar reply_to para no tener una unica cola de vuelta)

class CliManagerController:
    def __init__(self, rabbit_ip, requests_queue_name, sys_status_filename, last_row_filename):
        self.requests_queue_name = requests_queue_name
        
        self.persistor_state = Persistor(sys_status_filename)
        # self.persistor_last_row = Persistor(last_row_filename)
        self.system_state = self.reload_persisted_state() # (container_name | 'FREE')
        # self.last_row = self.reload_last_persisted_row()

        self.connection, self.channel = RabbitUtils.setup_connection_with_channel(rabbit_ip)
        self.request_queue = RabbitUtils.setup_queue(self.channel, self.requests_queue_name)
        
    

    def run(self):
        def on_request(ch, method, props, body): # La defino aca adentro para poder usar el self.
            # Parsear y chequear el input
            request_client_id = ApiPacketsEncoder.decode_bytes(body)
            
            reply_msg = 'SYSTEM_BUSY' 

            if self.system_state == request_client_id or self.system_state == 'FREE': 
                reply_msg = 'OK_TO_UPLOAD'
                self.system_state = request_client_id
                self.persistor_state.update(request_client_id)

            RabbitUtils.send_to_queue(ch, props.reply_to, reply_msg, props.correlation_id)

            ch.basic_ack(delivery_tag=method.delivery_tag)


        # setup input queue
        RabbitUtils.setup_input_queue(self.channel, self.requests_queue_name, on_request, auto_ack=False)

        logging.info('CLIMANAGER: Waiting for messages. To exit press CTRL+C')
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logging.warning('CLIMANAGER: ######### Received Ctrl+C! Stopping...')
            self.channel.stop_consuming()
        self.connection.close()

    def reload_persisted_state(self):
        persisted_state = self.persistor_state.read() 

        if not persisted_state: # Si el archivo esta corrupto, el sistema en cualquier caso debería actualizarse a libre
            self.persistor_state.update("FREE")
            return "FREE"
        else:
            # assert(len(persisted_state) == 2)
            return persisted_state[0]

    # def reload_last_persisted_row(self):
    #     persisted_last_rows = self.persistor_last_row.read()
    #     last_row = 0
    #     for event in persisted_last_rows:
    #         if event != "CHECK":
    #             last_row = int(event)    

    #     return last_row


    def _callback(self, ch, method, properties, body):
        # if BatchEncoderDecoder.is_encoded_sentinel(body):
        #     logging.info(f"FANOUT: Received sentinel! Shutting down...")
        #     self.partition_batcher.received_sentinel()
        #     raise KeyboardInterrupt

        # see tutorial for cli-svr replies:
        # https://www.rabbitmq.com/tutorials/tutorial-six-python.html

        cli_pkt = ApiPacketsEncoder.decode_bytes(body)
        
        # assume sys busy
        busy_pkt = ApiPacketsEncoder.create_sys_busy()

        RabbitUtils.send_to_queue(self.channel, props.reply_to, busy_pkt, corr_id=props.correlation_id)

        RabbitUtils.ack_from_method(self.channel, method)

        
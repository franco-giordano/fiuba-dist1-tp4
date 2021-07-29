import logging

from common.encoders.api_pkts_encoder_decoder import ApiPacketsEncoder
from common.encoders.obj_encoder_decoder import ObjectEncoderDecoder
from common.models.persistor import Persistor
from common.utils.rabbit_utils import RabbitUtils


class ResultsController:
    def __init__(self, id, rabbit_ip, output_queue, partial_persistance_filename, results_file):
        self.results_file = results_file
        self.connection, self.channel = RabbitUtils.setup_connection_with_channel(rabbit_ip)
        self.id = id

        self.persistor = Persistor(partial_persistance_filename)

        self.results_set = self._reload_init_results()

        # results queues
        RabbitUtils.setup_input_queue(self.channel, output_queue, self._callback, auto_ack=False)

    def run(self):
        logging.info(f'RESULTS {self.id}: Waiting for results. To exit press CTRL+C. Will write to {self.results_file}')
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logging.warning('RESULTS: ######### Received Ctrl+C! Stopping...')
            self.channel.stop_consuming()
        self.connection.close()

    def _reload_init_results(self):
        results_set = set()
        for row in self.persistor.read():
            if row != Persistor.CHECK_GUARD:
                results_set.add(row.strip())

        return results_set

    def _callback(self, ch, method, properties, body):
        data_recvd = ApiPacketsEncoder.decode_bytes(body)
        control_pkg = data_recvd.get('msg', None) if type(data_recvd) is dict else None
        logging.info(f"RESULTS: Received batch, writing result '{method.routing_key}, {body}\n'")

        if control_pkg == "[[INICIO]]":
            self.results_set = set()
            self.persistor.wipe()
        elif control_pkg == "[[FIN]]":
            with open(self.results_file, "w") as f:
                for row in self.results_set:
                    f.write(f"{row}\n")

            self.persistor.wipe()
            RabbitUtils.ack_from_method(self.channel, method)
            self.channel.stop_consuming()
            return
        else:
            row = ObjectEncoderDecoder.decode_bytes(body)
            encoded_row = ObjectEncoderDecoder.encode_obj_str(row)

            self.results_set.add(encoded_row)
            self.persistor.persist(encoded_row)

        RabbitUtils.ack_from_method(self.channel, method)


"""
RESULTS CONTROLLER
- Desencolo una fila
- Agrego en el set y Persisto
- 
- ACK de la fila

Si es FIN

- Desencolo
- Persisto todas las filas resultado
- ACK del FIN 
- Stop consuming
- Protocolo de limpieza para el siguiente dataset

"""

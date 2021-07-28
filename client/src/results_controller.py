from common.encoders.obj_encoder_decoder import ObjectEncoderDecoder
from common.encoders.batch_encoder_decoder import BatchEncoderDecoder
from common.utils.rabbit_utils import RabbitUtils
import logging
from common.models.persistor import Persistor

class ResultsController:
    def __init__(self, id, rabbit_ip, output_queue, partial_persistance_filename, results_file):
        self.results_file = results_file
        self.connection, self.channel = RabbitUtils.setup_connection_with_channel(rabbit_ip)
        self.id = id

        self.persistor = Persistor(partial_persistance_filename)

        self.result_set = self._reload_init_results()

        # results queues
        RabbitUtils.setup_input_queue(self.channel, output_queue, self._callback, False)

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
            if(row != Persistor.CHECK_GUARD):
                results_set.add(row.strip())

        return results_set

    def _callback(self, ch, method, properties, body):       
        batch = BatchEncoderDecoder.decode_bytes(body)
        logging.info(f"RESULTS: Received batch, writing result '{method.routing_key}, {batch}\n'")

        if batch == "[[INICIO]]":
            self.result_set = set()
            self.persistor.wipe()
        elif batch == "[[FIN]]":
            with open(self.results_file, "w") as f:
                for row in self.results_set:
                    f.write(f"{row}\n")
            RabbitUtils.ack_from_method(self.channel, method)
            self.channel.stop_consuming()
        else:
            encoded_batch = ObjectEncoderDecoder.encode_obj_str(batch)
            if encoded_batch not in self.results_file:
                self.results_file.write(encoded_batch)

            self.result_set.add(encoded_batch)
            self.persistor.persist(encoded_batch)

        # self.results_file.write(f'{method.routing_key}, {batch}\n')
        # self.results_file.flush()
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
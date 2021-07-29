import csv
import logging
import uuid
from multiprocessing import Process

from common.encoders.api_pkts_encoder_decoder import ApiPacketsEncoder
from common.encoders.batch_encoder_decoder import BatchEncoderDecoder
from common.encoders.match_encoder_decoder import MatchEncoderDecoder
from common.encoders.player_encoder_decoder import PlayerEncoderDecoder
from common.utils.rabbit_utils import RabbitUtils


class CSVDispatcher:
    def __init__(self, client_id, rabbit_ip, matches_fanout, matches_path, players_fanout, players_path, batch_size):
        self.rabbit_ip = rabbit_ip
        self.client_id = client_id
        self.able_to_upload = False

        self.BATCH_SIZE = batch_size

        self.corr_id = str(uuid.uuid4())
        self.connection, self.channel = RabbitUtils.setup_connection_with_channel(self.rabbit_ip)
        self.request_queue_name = "aoe2-requests"
        RabbitUtils.setup_queue(self.channel, self.request_queue_name)

        self.callback_queue = RabbitUtils.setup_anonym_input_queue(self.channel, self.on_response)

        self.matches_proc = Process(target=self.upload_csv, args=(matches_path, matches_fanout, MatchEncoderDecoder))
        self.players_proc = Process(target=self.upload_csv, args=(players_path, players_fanout, PlayerEncoderDecoder))

    def run(self):
        logging.info(f"Enviando {self.client_id} a la cola de requests")
        request_msg = ApiPacketsEncoder.create_request_pkt(self.client_id)
        RabbitUtils.send_to_queue(self.channel, self.request_queue_name, request_msg, self.corr_id, self.callback_queue)

        # while self.response is None:
        #     self.connection.process_data_events()
        self.channel.start_consuming()

        if self.able_to_upload:
            logging.info("El sistema se encuentra disponible, procedo a enviar los datasets")
            self.matches_proc.start()
            self.players_proc.start()

            self.matches_proc.join()
            self.players_proc.join()
        else:
            logging.info("El sistema se encuentra ocupado, intente mas tarde")

        return self.able_to_upload

    def on_response(self, ch, method, props, body):
        logging.info(f"Recibo respuesta {body}")
        if self.corr_id == props.correlation_id:
            reply = ApiPacketsEncoder.decode_bytes(body)['msg']
            logging.info(f"recibi decodeado {reply}")
            if reply == "OK_TO_UPLOAD":
                logging.info(f"recibo ok to upload y pasa el corr id")
                self.able_to_upload = True

        logging.info(f"Puedo subir: {self.able_to_upload}")
        self.channel.stop_consuming()

    def upload_csv(self, csv_path, fanout_name, decoder):
        logging.info(f'{fanout_name}: Arranco a mandar')
        connection, channel = RabbitUtils.setup_connection_with_channel(self.rabbit_ip)
        RabbitUtils.setup_fanout_exchange(channel, fanout_name)
        row_number = 1

        with open(csv_path, newline='') as csvf:
            reader = csv.DictReader(csvf)
            batch = []
            count = 0
            logging.info(f'{fanout_name}: Abri archivo')
            for row_dict in reader:
                parsed_dict = decoder.parse_dict(row_dict)
                parsed_dict["row_number"] = row_number
                batch.append(parsed_dict)
                count += 1

                if count >= self.BATCH_SIZE:
                    serialized = BatchEncoderDecoder.encode_batch(batch)
                    channel.basic_publish(exchange=fanout_name, routing_key='', body=serialized)
                    # logging.info(f"{fanout_name}: Sent batch {serialized[:25]}...")
                    batch = []
                    count = 0
                    # time.sleep(5)

                row_number += 1  # Id unico por fila

            if count > 0:
                serialized = BatchEncoderDecoder.encode_batch(batch)
                channel.basic_publish(exchange=fanout_name, routing_key='', body=serialized)
                # logging.info(f"{fanout_name}: Sent last missing batch {serialized[:25]}...")

        logging.info(f"{fanout_name}: Reached EOF, sending sentinel")
        sentinel_batch = BatchEncoderDecoder.create_encoded_sentinel()
        channel.basic_publish(exchange=fanout_name, routing_key='', body=sentinel_batch)

        connection.close()

    def finished_proccessing(self):
        logging.info(f"Notifico al cli-manager que el sistema ya se encuentra libre")
        request_msg = ApiPacketsEncoder.create_idle_sys_pkt(self.client_id)
        RabbitUtils.send_to_queue(self.channel, self.request_queue_name, request_msg, self.corr_id, self.callback_queue)
        self.connection.close()

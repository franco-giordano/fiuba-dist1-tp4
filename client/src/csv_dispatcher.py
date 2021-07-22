import logging
import csv
from multiprocessing import Process
from common.encoders.player_encoder_decoder import PlayerEncoderDecoder
from common.encoders.match_encoder_decoder import MatchEncoderDecoder
from common.encoders.batch_encoder_decoder import BatchEncoderDecoder
from common.encoders.api_pkts_encoder_decoder import ApiPacketsEncoder
from common.utils.rabbit_utils import RabbitUtils
import uuid
import pika


class CSVDispatcher:
    def __init__(self, client_id, rabbit_ip, matches_queue, matches_path, players_queue, players_path, batch_size):
        self.rabbit_ip = rabbit_ip
        self.client_id = client_id
        self.able_to_upload = False

        self.BATCH_SIZE = batch_size

        self.corr_id = str(uuid.uuid4())
        self.connection, self.channel = RabbitUtils.setup_connection_with_channel(self.rabbit_ip)
        self.request_queue = RabbitUtils.setup_queue(self.channel, "aoe2-requests")
        
        self.callback_queue = RabbitUtils.setup_anonym_input_queue(self.channel, self.on_response)
        
        self.matches_proc = Process(target=self.upload_csv, args=(matches_path, matches_queue, MatchEncoderDecoder))
        self.players_proc = Process(target=self.upload_csv, args=(players_path, players_queue, PlayerEncoderDecoder))

    def run(self):
        RabbitUtils.send_to_queue(self.channel, self.request_queue, f"{self.client_id}", self.corr_id, self.callback_queue)

        # while self.response is None:
        #     self.connection.process_data_events()
        self.channel.start_consuming()

        if self.able_to_upload:
            self.matches_proc.start()
            self.players_proc.start()

            self.matches_proc.join()
            self.players_proc.join()

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            reply = ApiPacketsEncoder.decode_bytes(body)
            if reply == "OK_TO_UPLOAD":
                self.able_to_upload = True

        ch.stop_consuming()         
            
    def upload_csv(self, csv_path, queue_name, decoder):
        connection, channel = RabbitUtils.setup_connection_with_channel(self.rabbit_ip)
        RabbitUtils.setup_queue(channel, queue_name)
        row_number = 1

        with open(csv_path, newline='') as csvf:
            reader = csv.DictReader(csvf)
            batch = []
            count = 0
            for row_dict in reader:
                parsed_dict = decoder.parse_dict(row_dict)
                parsed_dict["row_number"] = row_number
                batch.append(parsed_dict)
                count += 1

                if count >= self.BATCH_SIZE:
                    serialized = BatchEncoderDecoder.encode_batch(batch)
                    channel.basic_publish(exchange='', routing_key=queue_name, body=serialized)
                    logging.info(f"{queue_name}: Sent batch {serialized[:25]}...")
                    batch = []
                    count = 0
                    # time.sleep(5)
                
                row_number += 1 # Id unico por fila

            if count > 0:
                serialized = BatchEncoderDecoder.encode_batch(batch)
                channel.basic_publish(exchange='', routing_key=queue_name, body=serialized)
                logging.info(f"{queue_name}: Sent last missing batch {serialized[:25]}...")

        logging.info(f"{queue_name}: Reached EOF, sending sentinel")
        sentinel_batch = BatchEncoderDecoder.create_encoded_sentinel()
        channel.basic_publish(exchange='', routing_key=queue_name, body=sentinel_batch)

        connection.close()

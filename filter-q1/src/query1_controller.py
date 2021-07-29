import logging
from multiprocessing import Process

from src.filter_query1 import FilterQuery1

from common.encoders.api_pkts_encoder_decoder import ApiPacketsEncoder
from common.encoders.batch_encoder_decoder import BatchEncoderDecoder
from common.encoders.match_encoder_decoder import MatchEncoderDecoder
from common.models.persistor import Persistor
from common.utils.heartbeat import HeartBeat
from common.utils.rabbit_utils import RabbitUtils


class Query1Controller:
    def __init__(self, rabbit_ip, matches_exchange_name, output_queue_name, pongs_queue, filtered_rows_filename):
        self.matches_exchange_name = matches_exchange_name
        self.output_queue_name = output_queue_name
        self.pongs_queue = pongs_queue

        self.connection, self.channel = RabbitUtils.setup_connection_with_channel(
            rabbit_ip)

        # Seteo heartbeat para todos
        self.heartbeat_process = Process(target=self._heartbeat_init, args=(rabbit_ip,))

        # input exchange
        RabbitUtils.setup_input_fanout_exchange(
            self.channel, self.matches_exchange_name, self._callback, auto_ack=False)

        # output queue
        RabbitUtils.setup_queue(self.channel, self.output_queue_name)

        self.filtered_rows = set()

        self.filter = FilterQuery1()

        self.persistor = Persistor(filtered_rows_filename)

        persisted_state = self.persistor.read()

        for event in persisted_state:
            if event != Persistor.CHECK_GUARD:
                match = MatchEncoderDecoder.decode_str(event)
                self._preprocess(match)
        logging.info(f"FILTER QUERY1: rebuilt set now is {self.filtered_rows}")

    def run(self):
        self.heartbeat_process.start()

        logging.info('FILTER QUERY1: Waiting for messages. To exit press CTRL+C')
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logging.warning(
                'FILTER QUERY1: ######### Received Ctrl+C! Stopping...')
            self.channel.stop_consuming()
        self.connection.close()

    def _callback(self, ch, method, properties, body):
        if BatchEncoderDecoder.is_encoded_sentinel(body):
            logging.info(f"FILTER QUERY1: Received sentinel! Shutting down...")
            # mandar a cola final

            start_msg = ApiPacketsEncoder._encode_pkt({"msg": "[[INICIO]]"})
            end_msg = ApiPacketsEncoder._encode_pkt({"msg": "[[FIN]]"})

            self.channel.basic_publish(
                exchange='', routing_key=self.output_queue_name, body=start_msg)
            for serialized_match in self.filtered_rows:
                self.channel.basic_publish(
                    exchange='', routing_key=self.output_queue_name, body=serialized_match)
            self.channel.basic_publish(
                exchange='', routing_key=self.output_queue_name, body=end_msg)

            self.persistor.wipe()

            RabbitUtils.ack_from_method(self.channel, method)
            return

        batch = BatchEncoderDecoder.decode_bytes(body)
        passing = list(filter(self.filter.should_pass, batch))

        if passing:
            for match in passing:
                self.filtered_rows.add(
                    MatchEncoderDecoder.encode_match_str(match))

                logging.info(f"FILTER QUERY1: set now is {self.filtered_rows}")

                # persistir
                self.persistor.persist(
                    MatchEncoderDecoder.encode_match_str(match))

                # ack

        RabbitUtils.ack_from_method(self.channel, method)

    def _preprocess(self, match):
        can_pass = self.filter.should_pass(match)

        if can_pass:
            self.filtered_rows.add(MatchEncoderDecoder.encode_match_str(match))

    def _heartbeat_init(self, rabbit_ip):
        heartbeat = HeartBeat("filter-q1", rabbit_ip, self.pongs_queue)
        heartbeat.run()

# old:

# logging.info(f'FILTER QUERY1: Sending to output queue the passing matches {passing}')
# serialized = BatchEncoderDecoder.encode_batch(passing)
# self.channel.basic_publish(exchange='', routing_key=self.output_queue_name, body=serialized)

from multiprocessing import Process

from common.encoders.batch_encoder_decoder import BatchEncoderDecoder
from src.batched_filter_ladder import BatchedFilterLadder

from common.utils.heartbeat import HeartBeat
from common.utils.rabbit_utils import RabbitUtils
import logging

class LadderFilterController:
    def __init__(self, rabbit_ip, matches_exchange_name, output_exchange_name, pongs_queue, route_1v1, route_team, max_outgoing_sentinels):
        self.matches_exchange_name = matches_exchange_name
        self.output_exchange_name = output_exchange_name
        self.max_outgoing_sentinels = max_outgoing_sentinels
        self.pongs_queue = pongs_queue

        self.connection, self.channel = RabbitUtils.setup_connection_with_channel(rabbit_ip)

        # Seteo heartbeat para todos
        self.heartbeat_process = Process(target=self._heartbeat_init, args=(rabbit_ip,))

        # input exchange
        RabbitUtils.setup_input_fanout_exchange(self.channel, self.matches_exchange_name, self._callback)

        # output exchange
        RabbitUtils.setup_output_direct_exchange(self.channel, self.output_exchange_name)

        self.batched_filter = BatchedFilterLadder(route_1v1, route_team)

    def run(self):
        self.heartbeat_process.start()

        logging.info('FILTER BY LADDER: Waiting for messages. To exit press CTRL+C')
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logging.warning('FILTER BY LADDER: ######### Received Ctrl+C! Stopping...')
            self.channel.stop_consuming()
        self.connection.close()

    def _callback(self, ch, method, properties, body):
        if BatchEncoderDecoder.is_encoded_sentinel(body):
            logging.info(f"FILTER BY LADDER: Received sentinel! Propagating")
            for i in range(self.max_outgoing_sentinels):
                self.channel.basic_publish(exchange=self.output_exchange_name, \
                    routing_key=self.batched_filter.route_1v1, body=body)
                self.channel.basic_publish(exchange=self.output_exchange_name, \
                    routing_key=self.batched_filter.route_team, body=body)
            # raise KeyboardInterrupt
            return

        batch = BatchEncoderDecoder.decode_bytes(body)
        # logging.info(f"FILTER BY LADDER: Received batch {body[:25]}...")

        batch_1v1, batch_team = self.batched_filter.create_filtered_batches(batch)

        if batch_1v1:
            # logging.info(f'FILTER BY LADDER: Sending to output exchange matches for route 1v1')
            ser1 = BatchEncoderDecoder.encode_batch(batch_1v1)
            self.channel.basic_publish(exchange=self.output_exchange_name, routing_key=self.batched_filter.route_1v1, body=ser1)
        if batch_team:
            # logging.info(f'FILTER BY LADDER: Sending to output exchange matches for route TEAM')
            ser2 = BatchEncoderDecoder.encode_batch(batch_team)
            self.channel.basic_publish(exchange=self.output_exchange_name, routing_key=self.batched_filter.route_team, body=ser2)

    def _heartbeat_init(self, rabbit_ip):
        heartbeat = HeartBeat("filter-by-ladder", rabbit_ip, self.pongs_queue)
        heartbeat.run()



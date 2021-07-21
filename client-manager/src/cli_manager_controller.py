from common.utils.rabbit_utils import RabbitUtils
from common.encoders.api_pkts_encoder_decoder import ApiPacketsEncoder
import logging

class CliManagerController:
    def __init__(self, rabbit_ip, ping_queue_name, requests_queue_name, sys_status_filename):
        self.ping_queue_name = ping_queue_name
        self.requests_queue_name = requests_queue_name

        self.connection, self.channel = RabbitUtils.setup_connection_with_channel(rabbit_ip)

        # setup input queue
        RabbitUtils.setup_input_queue(self.channel, self.requests_queue_name, self._callback, auto_ack=False)
        RabbitUtils.setup_input_queue(self.channel, self.ping_queue_name, self._callback, auto_ack=False)

        # setup output exchange
        RabbitUtils.setup_output_queue(self.channel, self.replies_queue_name)

    def run(self):
        logging.info('CLIMANAGER: Waiting for messages. To exit press CTRL+C')
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logging.warning('CLIMANAGER: ######### Received Ctrl+C! Stopping...')
            self.channel.stop_consuming()
        self.connection.close()

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

        
from common.encoders.batch_encoder_decoder import BatchEncoderDecoder
from common.utils.master_utils import MasterUtils
import logging


class MasterController:
    def __init__(self, rabbit_ip, master_comms_exchange, my_master_id, masters_amount, log_filename=None):
        self.master_comms_exchange = master_comms_exchange
        self.my_master_id = my_master_id
        self.masters_amount = masters_amount
        self.current_leader = -1
        self.log_filename = log_filename

        self.connection, self.channel = MasterUtils.setup_connection_with_channel(
            rabbit_ip)

        # comms exchange with other masters
        MasterUtils.setup_master_comms(
            self.channel, master_comms_exchange, my_master_id, self._comms_callback)

    def run(self):
        logging.info('MASTER: Waiting for messages. To exit press CTRL+C')
        try:
            # para testear mando un msg incial
            MasterUtils.send_to_all_masters(
                self.channel,
                self.master_comms_exchange,
                self.my_master_id,
                f'primer hola desde {self.my_master_id}',
                self.masters_amount)

            self.channel.start_consuming()
        except KeyboardInterrupt:
            logging.warning('MASTER: ######### Received Ctrl+C! Stopping...')
            self.channel.stop_consuming()
        self.connection.close()

    def _comms_callback(self, ch, method, properties, body):
        # if BatchEncoderDecoder.is_encoded_sentinel(body):
        #     logging.info(f"FILTER QUERY1: Received sentinel! Shutting down...")
        #     raise KeyboardInterrupt
        logging.info(f"MASTER-{self.my_master_id}: Received msg '{body}'")

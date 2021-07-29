from time import sleep

from common.encoders.obj_encoder_decoder import ObjectEncoderDecoder
from common.utils.rabbit_utils import RabbitUtils


class HeartBeat:
    def __init__(self, node_id, rabbit_ip, heartbeat_queue_name):
        self.node_id = node_id.strip()
        self.rabbit_ip = rabbit_ip

        self.heartbeat_time = 1.5

        self.connection, self.channel = RabbitUtils.setup_connection_with_channel(self.rabbit_ip)
        self.heartbeat_queue_name = heartbeat_queue_name
        RabbitUtils.setup_queue(self.channel, heartbeat_queue_name)

    def run(self):
        while True:
            heartbeat_body = ObjectEncoderDecoder.encode_obj(self.node_id)
            RabbitUtils.send_to_queue(self.channel, self.heartbeat_queue_name, heartbeat_body)

            sleep(self.heartbeat_time)

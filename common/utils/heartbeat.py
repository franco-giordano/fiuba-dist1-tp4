from common.utils.rabbit_utils import RabbitUtils
from common.encoders.obj_encoder_decoder import ObjectEncoderDecoder
from time import sleep

class HeartBeat:
    def __init__(self, node_id, rabbit_ip, heartbeat_queue_name):
        self.node_id = node_id
        self.rabbit_ip = rabbit_ip

        self.heartbeat_time = 0.5

        self.connection, self.channel = RabbitUtils.setup_connection_with_channel(self.rabbit_ip)
        self.heartbeat_queue = RabbitUtils.setup_queue(self.channel, heartbeat_queue_name)

    def run(self):
        while True:
            heartbeat_body = ObjectEncoderDecoder.encode_obj(self.node_id)
            RabbitUtils.send_to_queue(self.channel, self.heartbeat_queue, heartbeat_body)
            
            sleep(self.heartbeat_time)






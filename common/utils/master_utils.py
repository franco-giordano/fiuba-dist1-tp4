import pika
from common.encoders.obj_encoder_decoder import ObjectEncoderDecoder

import logging
logging.getLogger("pika").propagate = False  # Para que pika no me llene los logs
class MasterUtils:
    @staticmethod
    def setup_connection_with_channel(rabbit_ip):
        conn = pika.BlockingConnection(
            pika.ConnectionParameters(host=rabbit_ip))
        channel = conn.channel()
        return conn, channel

    @staticmethod
    def setup_master_comms(channel, exchange_name, my_master_id, callback):
        channel.exchange_declare(
            exchange=exchange_name, exchange_type='topic')
        result = channel.queue_declare(queue='', exclusive=True)
        queue_name = result.method.queue
        # for i in range(masters_amount):
        #     if i != my_master_id:   # no me suscribo a mi propio id
        # me suscribo a todo lo que sea para mi
        channel.queue_bind(exchange=exchange_name,
                           queue=queue_name, routing_key=f"*.to_{my_master_id}")
        channel.basic_consume(
            queue=queue_name, on_message_callback=callback, auto_ack=True)

    @staticmethod
    def send_to_all_masters(channel, exchange_name, my_master_id, body, masters_amount):
        for i in range(masters_amount):
            if i != my_master_id:   # no mando a mi propio id
                channel.basic_publish(
                    exchange=exchange_name,
                    routing_key=f"from_{my_master_id}.to_{i}",
                    body=body)

    @staticmethod
    def send_to_greater_ids(channel, exchange_name, my_master_id, body, masters_amount):
        for i in range(my_master_id+1, masters_amount): # nodes with id greater than mine
            channel.basic_publish(
                exchange=exchange_name,
                routing_key=f"from_{my_master_id}.to_{i}",
                body=body)

    @staticmethod
    def send_to_id(channel, exchange_name, my_master_id, body, other_id):
        channel.basic_publish(
            exchange=exchange_name,
            routing_key=f"from_{my_master_id}.to_{other_id}",
            body=body)

    @staticmethod
    def send_alive_bully_msg(channel, exchange_name, my_master_id, dest_master_id):
        channel.basic_publish(
            exchange=exchange_name,
            routing_key=f"from_{my_master_id}.to_{dest_master_id}",
            body=ObjectEncoderDecoder.encode_obj({"type": "[[ALIVE]]", "id": my_master_id}))

    @staticmethod
    def send_one_coordinator_msg(channel, exchange_name, my_master_id, dest_master_id):
        coord_msg = ObjectEncoderDecoder.encode_obj({"type": "[[COORDINATOR]]", "id": my_master_id})
        channel.basic_publish(
            exchange=exchange_name,
            routing_key=f"from_{my_master_id}.to_{dest_master_id}",
            body=coord_msg)


    @staticmethod
    def ack_from_method(channel, method):
        channel.basic_ack(delivery_tag=method.delivery_tag)

import pika


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
    def ack_from_method(channel, method):
        channel.basic_ack(delivery_tag=method.delivery_tag)

import pika


class MasterUtils:
    @staticmethod
    def setup_connection_with_channel(rabbit_ip):
        conn = pika.BlockingConnection(
            pika.ConnectionParameters(host=rabbit_ip))
        channel = conn.channel()
        return conn, channel

    @staticmethod
    def setup_master_comms(channel, exchange_name, my_master_id, masters_amount, callback):
        channel.exchange_declare(
            exchange=exchange_name, exchange_type='direct')
        result = channel.queue_declare(queue='', exclusive=True)
        queue_name = result.method.queue
        for i in range(masters_amount):
            if i != my_master_id:   # no me suscribo a mi propio id
                channel.queue_bind(exchange=exchange_name,
                                   queue=queue_name, routing_key=f"from-{i}")
        channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

    @staticmethod
    def send_to_masters(channel, exchange_name, my_master_id, body):
        channel.basic_publish(
            exchange=exchange_name, routing_key=f"from-{my_master_id}", body=body)

    @staticmethod
    def ack_from_method(channel, method):
        channel.basic_ack(delivery_tag=method.delivery_tag)

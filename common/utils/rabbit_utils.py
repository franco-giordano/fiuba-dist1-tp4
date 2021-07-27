import pika
import logging
# logging.getLogger("pika").propagate = False

class RabbitUtils:
    @staticmethod
    def setup_connection_with_channel(rabbit_ip):
        conn = pika.BlockingConnection(pika.ConnectionParameters(host=rabbit_ip))
        channel = conn.channel()
        return conn, channel

    @staticmethod
    def setup_input_direct_exchange(channel, exchange_name, routing_key, callback, queue_name=None, auto_ack=True):
        channel.exchange_declare(exchange=exchange_name, exchange_type='direct')
        if not queue_name:
            result = channel.queue_declare(queue='', exclusive=True)
            queue_name = result.method.queue
        else:
            channel.queue_declare(queue=queue_name, exclusive=False)
        channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key=routing_key)
        channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=auto_ack)

    @staticmethod
    def setup_output_direct_exchange(channel, exchange_name):
        channel.exchange_declare(exchange=exchange_name, exchange_type='direct')

    @staticmethod
    def setup_input_fanout_exchange(channel, exchange_name, callback, auto_ack=True):
        RabbitUtils.setup_fanout_exchange(channel, exchange_name)
        result = channel.queue_declare(queue='', exclusive=True)
        queue_name = result.method.queue
        channel.queue_bind(exchange=exchange_name, queue=queue_name)
        channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=auto_ack)

    @staticmethod
    def setup_fanout_exchange(channel, exchange_name):
        channel.exchange_declare(exchange=exchange_name, exchange_type='fanout')

    @staticmethod
    def setup_queue(channel, queue_name):
        channel.queue_declare(queue=queue_name)

    @staticmethod
    def setup_output_queue(channel, queue_name):
        channel.queue_declare(queue=queue_name)

    @staticmethod
    def setup_input_queue(channel, queue_name, callback, auto_ack=True):
        RabbitUtils.setup_queue(channel, queue_name)
        channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=auto_ack)

    @staticmethod
    def setup_anonym_input_queue(channel, callback):
        result = channel.queue_declare(queue='', exclusive=True)
        callback_queue = result.method.queue

        channel.basic_consume(
            queue=callback_queue,
            on_message_callback=callback,
            auto_ack=True)

        return callback_queue

    @staticmethod
    def send_to_queue(channel, queue_name, body, corr_id=None, reply_queue=None):
        props = None
        if corr_id:
            props=pika.BasicProperties(correlation_id = corr_id, reply_to=reply_queue)
        channel.basic_publish(exchange='',
            routing_key=queue_name,
            properties=props,
            body=body)

    @staticmethod
    def ack_from_method(channel, method):
        channel.basic_ack(delivery_tag=method.delivery_tag)

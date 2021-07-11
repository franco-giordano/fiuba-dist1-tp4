import pika

class RabbitUtils:
    @staticmethod
    def setup_connection_with_channel(rabbit_ip):
        conn = pika.BlockingConnection(pika.ConnectionParameters(host=rabbit_ip))
        channel = conn.channel()
        return conn, channel

    @staticmethod
    def setup_input_direct_exchange(channel, exchange_name, routing_key, callback, queue_name=None):
        channel.exchange_declare(exchange=exchange_name, exchange_type='direct')
        if not queue_name:
            result = channel.queue_declare(queue='', exclusive=True)
            queue_name = result.method.queue
        else:
            channel.queue_declare(queue=queue_name, exclusive=False)
        channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key=routing_key)
        channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

    @staticmethod
    def setup_output_direct_exchange(channel, exchange_name):
        channel.exchange_declare(exchange=exchange_name, exchange_type='direct')

    @staticmethod
    def setup_input_fanout_exchange(channel, exchange_name, callback):
        RabbitUtils.setup_fanout_exchange(channel, exchange_name)
        result = channel.queue_declare(queue='', exclusive=True)
        queue_name = result.method.queue
        channel.queue_bind(exchange=exchange_name, queue=queue_name)
        channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

    @staticmethod
    def setup_fanout_exchange(channel, exchange_name):
        channel.exchange_declare(exchange=exchange_name, exchange_type='fanout')

    @staticmethod
    def setup_queue(channel, queue_name):
        channel.queue_declare(queue=queue_name)

    @staticmethod
    def setup_input_queue(channel, queue_name, callback):
        RabbitUtils.setup_queue(channel, queue_name)
        channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

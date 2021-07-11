from common.encoders.batch_encoder_decoder import BatchEncoderDecoder
import logging

class PartitionBatcher:
    def __init__(self, channel, exchange_name, max_batch_size):
        self.channel = channel
        self.exchange_name = exchange_name
        self.max_batch_size = max_batch_size

    def send_batch(self, batch):
        if len(batch) <= self.max_batch_size:
            logging.info(f'PARTITION BATCHER: Sending batch without change')
            serialized = BatchEncoderDecoder.encode_batch(batch)
            self.channel.basic_publish(exchange=self.exchange_name, routing_key='', body=serialized)
        else:
            self._partition_and_send(batch)
        
    def _partition_and_send(self, batch):
        partitions_count = len(batch) // self.max_batch_size + 1
        for i in range(partitions_count):
            part = batch[i*self.max_batch_size:(i+1)*self.max_batch_size]
            if part:
                logging.info(f'PARTITION BATCHER: Sending partitioned batch {i}')
                serialized = BatchEncoderDecoder.encode_batch(part)
                self.channel.basic_publish(exchange=self.exchange_name, routing_key='', body=serialized)

    def received_sentinel(self):
        sentinel = BatchEncoderDecoder.create_encoded_sentinel()
        self.channel.basic_publish(exchange=self.exchange_name, routing_key='', body=sentinel)

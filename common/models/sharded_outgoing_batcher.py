from common.encoders.batch_encoder_decoder import BatchEncoderDecoder
from common.models.shard_key_getter import ShardKeyGetter
import logging

class ShardedOutgoingBatcher:
    def __init__(self, rabbit_channel, reducers_amount, batch_size, output_exchange_name, tkn_key='match'):
        self.all_outgoing_batches = {}
        self.shard_key_getter = ShardKeyGetter(reducers_amount)
        self.channel = rabbit_channel
        self.max_batch_size = batch_size
        self.output_exchange_name = output_exchange_name
        self.tkn_key = tkn_key

    def add_to_batch(self, item):
        shard_key = self.shard_key_getter.get_key_for_str(item[self.tkn_key])
        
        batch = self.all_outgoing_batches.get(shard_key, [])
        batch.append(item)
        self.all_outgoing_batches[shard_key] = batch

    def publish_if_full(self):
        for shard_key in list(self.all_outgoing_batches):
            batch = self.all_outgoing_batches[shard_key]
            if len(batch) >= self.max_batch_size:
                logging.info(f'SHARD EXCHANGE: Announcing batch for shard key {shard_key}')
                serialized = BatchEncoderDecoder.encode_batch(batch)
                self.channel.basic_publish(exchange=self.output_exchange_name, routing_key=shard_key, body=serialized)
                del self.all_outgoing_batches[shard_key]

    def received_sentinel(self):
        logging.info(f'SHARD EXCHANGE: Flushing all batches...')
        self._flush_all_batches()

        logging.info(f'SHARD EXCHANGE: Sending sentinels to all shards...')
        sentinel = BatchEncoderDecoder.create_encoded_sentinel()
        all_keys = self.shard_key_getter.generate_all_shard_keys()
        for key in all_keys:
            self.channel.basic_publish(exchange=self.output_exchange_name, routing_key=key, body=sentinel)

    def _flush_all_batches(self):
        for shard_key, batch in self.all_outgoing_batches.items():
            if batch:
                serialized = BatchEncoderDecoder.encode_batch(batch)
                self.channel.basic_publish(exchange=self.output_exchange_name, routing_key=shard_key, body=serialized)

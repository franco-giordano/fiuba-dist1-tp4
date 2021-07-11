from common.encoders.obj_encoder_decoder import ObjectEncoderDecoder

class BatchEncoderDecoder:
    SENTINEL = ['SENTINEL']
    ENCODED_SENTINEL = ObjectEncoderDecoder.encode_obj(SENTINEL)

    @staticmethod
    def encode_batch(batch):
        return ObjectEncoderDecoder.encode_obj(batch)

    @staticmethod
    def decode_bytes(bytes_recv):
        return ObjectEncoderDecoder.decode_bytes(bytes_recv)

    @classmethod
    def create_encoded_sentinel(cls):
        return cls.ENCODED_SENTINEL

    @classmethod
    def is_encoded_sentinel(cls, bytes_recv):
        return cls.ENCODED_SENTINEL == bytes_recv

    @classmethod
    def is_players_batch(cls, decoded_batch):
        return 'match' in decoded_batch[0]

    @classmethod
    def is_matches_batch(cls, decoded_batch):
        return not cls.is_players_batch(decoded_batch)

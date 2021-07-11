import json

class ObjectEncoderDecoder:
    @staticmethod
    def encode_obj(obj):
        return json.dumps(obj).encode('utf-8')

    @staticmethod
    def decode_bytes(bytes_recv):
        return json.loads(bytes_recv.decode('utf-8'))

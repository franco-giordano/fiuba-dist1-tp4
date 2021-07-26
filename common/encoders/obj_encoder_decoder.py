import json

class ObjectEncoderDecoder:
    @staticmethod
    def encode_obj(obj):
        return json.dumps(obj).encode('utf-8')
        # TODO: creo que habria que sacar el .encode()
        #   porque estariamos mandando mitad strings mitad bytes a veces
        #   (INI-FIN mensajes str vs filas bytes)

    @staticmethod
    def decode_bytes(bytes_recv):
        return json.loads(bytes_recv.decode('utf-8'))

    @staticmethod
    def encode_obj_str(obj):
        return json.dumps(obj)

    @staticmethod
    def decode_str(str_recv):
        return json.loads(str_recv)

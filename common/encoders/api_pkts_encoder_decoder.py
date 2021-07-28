from common.encoders.obj_encoder_decoder import ObjectEncoderDecoder

class ApiPacketsEncoder:
    @staticmethod
    def _encode_pkt(pkt):
        return ObjectEncoderDecoder.encode_obj(pkt)

    @staticmethod
    def decode_bytes(bytes_recv):
        return ObjectEncoderDecoder.decode_bytes(bytes_recv)

    @classmethod
    def create_ok_to_upload(cls): # TODO: Usar quizas estos metodos para el cli mngr
        return cls._encode_pkt({'msg': 'OK_TO_UPLOAD'})

    @classmethod
    def create_sys_busy(cls):
        return cls._encode_pkt({'msg': 'SYSTEM_BUSY'})

    @classmethod
    def create_request_pkt(cls, node_name):
        return cls._encode_pkt({'id': node_name})

    @classmethod
    def is_control_pkt(cls, decoded_pkt):
        return 'msg' in decoded_pkt

    @classmethod
    def create_inicio_pkt(cls):
        return cls._encode_pkt({'msg': '[[INICIO]]'})

    @classmethod
    def create_fin_pkt(cls):
        return cls._encode_pkt({'msg': '[[FIN]]'})

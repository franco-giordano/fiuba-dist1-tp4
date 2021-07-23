from common.encoders.obj_encoder_decoder import ObjectEncoderDecoder

class ApiPacketsEncoder:
    @staticmethod
    def _encode_pkt(pkt):
        return ObjectEncoderDecoder.encode_obj(pkt)

    @staticmethod
    def decode_bytes(bytes_recv):
        return ObjectEncoderDecoder.decode_bytes(bytes_recv)

    @classmethod
    def create_ok_to_upload(cls, uuid): # TODO: Usar quizas estos metodos para el cli mngr
        return cls._encode_pkt({'uuid': uuid, 'msg': 'OK_TO_UPLOAD'})

    @classmethod
    def create_sys_busy(cls, uuid):
        return cls._encode_pkt({'uuid': uuid, 'msg': 'SYSTEM_BUSY'})

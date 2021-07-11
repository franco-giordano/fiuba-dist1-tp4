from common.encoders.obj_encoder_decoder import ObjectEncoderDecoder

class PlayerEncoderDecoder:
    @staticmethod
    def decode_bytes(bytes_recv: bytes) -> dict:
        parsed = ObjectEncoderDecoder.decode_bytes(bytes_recv)
        return PlayerEncoderDecoder.parse_dict(parsed)

    @staticmethod
    def encode_player(dict_recv: dict) -> bytes:
        return ObjectEncoderDecoder.encode_obj(dict_recv)

    @staticmethod
    def parse_dict(dict_recv: dict) -> dict:
        shallow_copy = dict_recv.copy()
        shallow_copy['team'] = int(shallow_copy['team'])
        shallow_copy['winner'] = shallow_copy['winner'] == 'True'
        shallow_copy['rating'] = int(shallow_copy['rating']) if shallow_copy['rating'] else 0
        return shallow_copy

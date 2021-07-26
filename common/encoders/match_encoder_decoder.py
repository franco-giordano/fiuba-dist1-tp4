from common.encoders.obj_encoder_decoder import ObjectEncoderDecoder

class MatchEncoderDecoder:
    @staticmethod
    def decode_bytes(bytes_recv: bytes) -> dict:
        return ObjectEncoderDecoder.decode_bytes(bytes_recv)

    @staticmethod
    def encode_match(dict_recv: dict) -> bytes:
        return ObjectEncoderDecoder.encode_obj(dict_recv)

    @staticmethod
    def decode_str(str_recv: str) -> dict:
        return ObjectEncoderDecoder.decode_str(str_recv)

    @staticmethod
    def encode_match_str(dict_recv: dict) -> str:
        return ObjectEncoderDecoder.encode_obj_str(dict_recv)

    @staticmethod
    def parse_dict(dict_recv: dict) -> dict:
        shallow_copy = dict_recv.copy()
        shallow_copy['winning_team'] = int(shallow_copy['winning_team'])
        shallow_copy['patch'] = int(shallow_copy['patch'])
        shallow_copy['num_players'] = int(shallow_copy['num_players'])
        shallow_copy['mirror'] = shallow_copy['mirror'] == 'True'
        shallow_copy['average_rating'] = int(shallow_copy['average_rating']) if shallow_copy['average_rating'] else 0
        shallow_copy['duration'] = parse_duration_as_seconds(shallow_copy['duration'])
        return shallow_copy

def parse_duration_as_seconds(dur):
    by_comma = dur.split(', ')
    if len(by_comma) == 1:
        nums = list(map(lambda x: int(x), by_comma[0].split(':')))
        return nums[0]*3600 + nums[1]*60 + nums[2]
    else:
        days = int(by_comma[0].split(' ')[0])
        nums = list(map(lambda x: int(x), by_comma[1].split(':')))
        return days*3600*24 + nums[0]*3600 + nums[1]*60 + nums[2]

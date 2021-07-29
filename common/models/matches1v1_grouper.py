import logging

from common.encoders.api_pkts_encoder_decoder import ApiPacketsEncoder
from common.encoders.batch_encoder_decoder import BatchEncoderDecoder
from common.encoders.obj_encoder_decoder import ObjectEncoderDecoder
from common.models.filter_query2 import FilterQuery2
from common.models.persistor import Persistor
from common.utils.rabbit_utils import RabbitUtils


class Matches1v1Grouper:
    def __init__(self, id_grouper, channel, output_queue_name, persistance_file):
        self.id_grouper = id_grouper
        self.channel = channel
        self.output_queue_name = output_queue_name
        self.persistance_file = persistance_file

        self.current_matches = {}
        self.persistor = Persistor(persistance_file)
        self.last_filter = FilterQuery2()

        self._reload_persisted_state()

    def add_player_in_match(self, player):
        match_tkn = player['match']

        if match_tkn in self.current_matches:
            if self.current_matches[match_tkn]:
                self.current_matches[match_tkn].append(player)

                if len(self.current_matches[match_tkn]) > 2:
                    self.current_matches[match_tkn] = None
        else:
            self.current_matches[match_tkn] = [player]

    def recv_msg(self, body):
        if BatchEncoderDecoder.is_encoded_sentinel(body):
            logging.info(f'MATCH 1v1 GROUPER: Flushing all grouped matches')
            self.flush_1v1_matches()

        else:
            player = ObjectEncoderDecoder.decode_str(body)[0]  # Batch de un solo player
            self.add_player_in_match(player)

            self.persistor.persist(ObjectEncoderDecoder.encode_obj_str(player))

    def flush_1v1_matches(self):
        inicio_msg = ApiPacketsEncoder.create_inicio_pkt()
        RabbitUtils.send_to_queue(self.channel, self.output_queue_name, inicio_msg, headers={'id': self.id_grouper})

        for tkn, players in self.current_matches.items():
            if players and len(players) == 2 and self.last_filter.should_pass(players):
                logging.info(f'MATCH 1v1 GROUPER: Announcing players for match {tkn}')
                serialized = BatchEncoderDecoder.encode_batch(players)

                RabbitUtils.send_to_queue(self.channel, self.output_queue_name, serialized,
                                          headers={'id': self.id_grouper})

        self.persistor.persist("FINISH")

        fin_msg = ApiPacketsEncoder.create_fin_pkt()
        RabbitUtils.send_to_queue(self.channel, self.output_queue_name, fin_msg, headers={'id': self.id_grouper})

        self.current_matches = {}
        self.persistor.wipe()

    def _reload_persisted_state(self):
        prev_data = self.persistor.read()
        for row in prev_data:
            if row == Persistor.CHECK_GUARD:
                continue
            player = ObjectEncoderDecoder.decode_str(row)

            self.add_player_in_match(player)

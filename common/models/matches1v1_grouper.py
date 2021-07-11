import logging
from common.encoders.batch_encoder_decoder import BatchEncoderDecoder
from common.models.filter_query2 import FilterQuery2

class Matches1v1Grouper:
    def __init__(self, channel, output_queue_name):
        self.channel = channel
        self.output_queue_name = output_queue_name
        self.current_matches = {}
        self.last_filter = FilterQuery2()

    def add_player_in_match(self, player):
        match_tkn = player['match']

        if match_tkn in self.current_matches:
            if self.current_matches[match_tkn]:
                self.current_matches[match_tkn].append(player)

                if len(self.current_matches[match_tkn]) > 2:
                    self.current_matches[match_tkn] = None
        else:
            self.current_matches[match_tkn] = [player]

    def received_sentinel(self):
        logging.info(f'MATCH 1v1 GROUPER: Flushing all grouped matches')
        self.flush_1v1_matches()

    def flush_1v1_matches(self):
        for tkn,players in self.current_matches.items():
            if players and len(players) == 2 and self.last_filter.should_pass(players):
                logging.info(f'MATCH 1v1 GROUPER: Announcing players for match {tkn}')
                serialized = BatchEncoderDecoder.encode_batch(players)
                self.channel.basic_publish(exchange='', routing_key=self.output_queue_name, body=serialized)

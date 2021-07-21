from common.encoders.batch_encoder_decoder import BatchEncoderDecoder
from common.models.shard_key_getter import ShardKeyGetter
import logging
from common.models.persistor import Persistor
import json
from common.utils.rabbit_utils import RabbitUtils

class MatchesJoiner:
    def __init__(self, id_joiner, channel, output_exchange_name, next_reducers_amount, force_send_on_first_join=False, persistance_file):
        self.channel = channel
        self.output_exchange_name = output_exchange_name
        self.id_joiner = id_joiner
        self.current_matches = {}
        self.persistance_file = persistance_file
        self.shard_key_getter = ShardKeyGetter(next_reducers_amount)
        self.force_send_on_first_join = force_send_on_first_join


        self.persistor = Persistor(self.persistance_file)

        persisted_state = self.persistor.read()

        for event in persisted_state:
            if event != "CHECK":
                player = json.loads(event)
                self._add_player(player)
                # TODO: Ver si es necesario agregar el match

    def add_players_batch(self, batch):
        for p in batch:
            self._add_player(p)

    def _add_player(self, player):
        tkn = player['match']
        joined_info = self.current_matches.get(tkn, [None, []])
        joined_info[1].append(player)
        self._send_or_store(joined_info, tkn)

    def _add_match(self, match):
        tkn = match['token']
        joined_info = self.current_matches.get(tkn, [None, []])
        assert(joined_info[0] is None)
        joined_info[0] = match
        self._send_or_store(joined_info, tkn)

        # En algun momento aca hay que mandar el INICIO, pero no tengo idea cuando es el primer mensaje con esta logica


    def _send_or_store(self, joined_info, token):
        if self._should_send(joined_info):
            
            shard_key = self.shard_key_getter.get_key_for_str(token)
            serialized = BatchEncoderDecoder.encode_batch(joined_info)
            logging.info(f'MATCHES JOINER: Found join for match {joined_info}, sending to shard key {shard_key}')
            self.channel.basic_publish(exchange=self.output_exchange_name, routing_key=shard_key, body=serialized)
            self._remove_info_if_possible(token)
        else:
            self.current_matches[token] = joined_info

            self.persistor.persist(json.dumps(joined_info)) # TODO: Preguntar bien como guarda franco las cosas aca.


    def _remove_info_if_possible(self, token):
        if not self.force_send_on_first_join:
            del self.current_matches[token]
        else:
            self.current_matches[token][1] = [] # reset match players

    def add_matches_batch(self, batch):
        for m in batch:
            self._add_match(m)

    def _should_send(self, joined_info):
        assert(len(joined_info[1])<=joined_info[0]['num_players'] if joined_info[0] is not None else True)

        if not self.force_send_on_first_join:
            return joined_info[0] is not None and len(joined_info[1])==joined_info[0]['num_players']
        else:
            return joined_info[0] is not None and len(joined_info[1])>=1

    def received_sentinel(self):
        logging.info(f'MATCHES JOINER: Propagating sentinel to group-by nodes...')
        sentinel = BatchEncoderDecoder.create_encoded_sentinel()
        all_keys = self.shard_key_getter.generate_all_shard_keys()
        for key in all_keys:
            # TODO: preguntar que es esto, no se que hacen las shard keys aca
            self.channel.basic_publish(exchange=self.output_exchange_name, routing_key=key, body=f"FIN {self.id_joiner}")

            self.persistor.persist("FINISH")
            self.persistor.wipe()
            # self.channel.basic_publish(exchange=self.output_exchange_name, routing_key=key, body=sentinel)

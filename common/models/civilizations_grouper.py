import logging

from common.encoders.api_pkts_encoder_decoder import ApiPacketsEncoder
from common.encoders.batch_encoder_decoder import BatchEncoderDecoder
from common.encoders.obj_encoder_decoder import ObjectEncoderDecoder
from common.models.persistor import Persistor
from common.utils.rabbit_utils import RabbitUtils


class CivilizationsGrouper:
    def __init__(self, id_grouper, channel, output_queue_name, aggregator, joiners_amount, persistance_file):
        self.channel = channel
        self.output_queue_name = output_queue_name
        self.id_grouper = id_grouper
        self.persistance_file = persistance_file
        self.aggregator = aggregator
        self.joiners_amount = joiners_amount

        self.data_per_joiner = {}
        self.joiner_state_machine = {str(key): 'WAITING_FOR_INICIO'
                                     for key in range(joiners_amount)}

        self.persistors = {str(key): Persistor(f'{persistance_file}-{self.id_grouper}-{key}')
                           for key in range(self.joiners_amount)}
        self._reload_persisted_state()

    def _reload_persisted_state(self):
        for from_id, persistor in self.persistors.items():
            prev_data = persistor.read()
            # for event in persisted_state:
            #     if event != Persistor.CHECK_GUARD:
            #         player = json.loads(event)
            #         self._add_player(player)
            for row in prev_data:
                if row == Persistor.CHECK_GUARD:
                    continue
                msg_type = row.strip()
                if msg_type == 'INICIO':
                    self.data_per_joiner[from_id] = {}
                    self.joiner_state_machine[from_id] = 'RECVING_ROWS'
                elif msg_type == 'FIN':
                    if self.joiner_state_machine[from_id] == 'RECVING_ROWS':
                        self.joiner_state_machine[from_id] = 'FIN_RECVED'
                else:  # es un dato del dataset
                    self._add_player(from_id, ObjectEncoderDecoder.decode_str(msg_type))

    def _add_player(self, from_id, player):
        civ = player['civ']
        prev_val = self.data_per_joiner[from_id].get(civ, None)
        self.data_per_joiner[from_id][civ] = self.aggregator.add(prev_val, player)

    def recv_msg(self, data, properties):
        from_id = properties.headers['id']

        if ApiPacketsEncoder.is_control_pkt(data):
            msg_type = data['msg']
            if msg_type == '[[INICIO]]':
                # limpio memoria
                self.data_per_joiner[from_id] = {}

                # no importa si estoy en WAITING o RECVING
                self.joiner_state_machine[from_id] = 'RECVING_ROWS'

                # wipe persistencia
                self.persistors[from_id].wipe()
                self.persistors[from_id].persist("INICIO")
            elif msg_type == '[[FIN]]':
                if self.joiner_state_machine[from_id] == 'RECVING_ROWS':
                    self.joiner_state_machine[from_id] = 'FIN_RECVED'
                    self.persistors[from_id].persist("FIN")

                if self._recved_all_fins():
                    self.flush_results()

        else:  # es un dato del dataset = joined_match
            # agrega en mem
            self.add_joined_match(from_id, data)

    def add_joined_match(self, from_id, joined_match):
        for player in joined_match[1]:
            self._add_player(from_id, player)
            serialized_player = ObjectEncoderDecoder.encode_obj_str(player)
            self.persistors[from_id].persist(serialized_player)

    # def recved_all_sentinels(self):
    #     logging.info(f'CIVS GROUPER: Flushing all grouped civs')
    #
    #     if "FINISH\n" in self.persistor.read() or not self.persistor.read():
    #         # Ignorar el sentinel directamente
    #         self.persistor.wipe()
    #         return
    #
    #     self.flush_results()
    #     logging.info(f'CIV GROUPER: Sending sentinel...')

    # sentinel = BatchEncoderDecoder.create_encoded_sentinel()
    # self.channel.basic_publish(exchange='', routing_key=self.output_queue_name, body=sentinel)

    def flush_results(self):
        civs_from_all_joiners = {}
        for key, value in self.data_per_joiner.items():  # value : {'civ1': [players...], 'civ2: [players...]}
            for civ, players in value.items():
                if civ not in civs_from_all_joiners:
                    civs_from_all_joiners[civ] = players[:]
                else:
                    civs_from_all_joiners[civ].extend(players)

        inicio_msg = ApiPacketsEncoder.create_inicio_pkt()
        RabbitUtils.send_to_queue(self.channel, self.output_queue_name, inicio_msg, headers={'id': self.id_grouper})
        for civ, players_list in civs_from_all_joiners.items():
            results = self.aggregator.collapse(players_list)
            logging.info(f'CIVS GROUPER: Announcing for civ {civ} the results {results}')
            serialized = BatchEncoderDecoder.encode_batch([civ, results])
            RabbitUtils.send_to_queue(self.channel, self.output_queue_name, serialized, headers={'id': self.id_grouper})

        for k in self.persistors:
            self.persistors[k].persist("FINISH")

        fin_msg = ApiPacketsEncoder.create_fin_pkt()
        RabbitUtils.send_to_queue(self.channel, self.output_queue_name, fin_msg, headers={'id': self.id_grouper})

        self._reset_variables()
        self._wipe_persistance()

    def _recved_all_fins(self):
        count = 0
        for state in self.joiner_state_machine.values():
            count += int(state == 'FIN_RECVED')

        return count == self.joiners_amount

    def _reset_variables(self):
        for k in self.joiner_state_machine:
            self.joiner_state_machine[k] = 'WAITING_FOR_INICIO'
        for k in self.data_per_joiner:
            self.data_per_joiner[k] = {}

    def _wipe_persistance(self):
        for persistor in self.persistors.values():
            persistor.wipe()


"""
[APPEND (mongol, id_fila_1), CHECK, APPEND (romanos, id_fila_2), CHECK, APPEND (romanos, id_fila_4)]

{“mongol” : set(ids)} dict[mongol].add(id_fila_2)

Leo una fila OK
Proceso OK
APPEND fila OK
CHECK OK
ACK input OK

"""

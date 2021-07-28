from common.utils.rabbit_utils import RabbitUtils
from common.models.persistor import Persistor
import logging
import json
from common.encoders.batch_encoder_decoder import BatchEncoderDecoder
from common.encoders.api_pkts_encoder_decoder import ApiPacketsEncoder

class CivilizationsGrouper:
    def __init__(self, id_grouper, channel, output_queue_name, aggregator, persistance_file):
        self.channel = channel
        self.output_queue_name = output_queue_name
        self.id_grouper = id_grouper
        self.current_civs = {}
        self.persistance_file = persistance_file
        self.aggregator = aggregator
        self.persistor = Persistor(self.persistance_file)
        
        persisted_state = self.persistor.read() 

        for event in persisted_state:
            if event != Persistor.CHECK_GUARD:
                player = json.loads(event)
                self._add_player(player)
                
    def _add_player(self, player):
        civ = player['civ']
        prev_val = self.current_civs.get(civ, None)
        self.current_civs[civ] = self.aggregator.add(prev_val, player)

    def add_joined_match(self, joined_match):
        for player in joined_match[1]:
            civ = player['civ']
            prev_val = self.current_civs.get(civ, None)
            self.current_civs[civ] = self.aggregator.add(prev_val, player)

            self.persistor.persist(json.dumps(player)) # Agrego APPEND

    def received_sentinel(self):
        logging.info(f'CIVS GROUPER: Flushing all grouped civs')

        if("FINISH\n" in self.persistor.read() or not self.persistor.read()):
            # Ignorar el sentinel directamente
            self.persistor.wipe()
            return
        self.flush_results()
        logging.info(f'CIV GROUPER: Sending sentinel...')

        # sentinel = BatchEncoderDecoder.create_encoded_sentinel()
        # self.channel.basic_publish(exchange='', routing_key=self.output_queue_name, body=sentinel)

    def flush_results(self):
        inicio_msg = ApiPacketsEncoder.create_inicio_pkt()
        RabbitUtils.send_to_queue(self.channel, self.output_queue_name, inicio_msg, headers={'id':self.id_grouper})
        # self.channel.basic_publish(exchange='', routing_key=self.output_queue_name, body=f"INICIO {self.id_grouper}")
        for civ, players_list in self.current_civs.items():
            results = self.aggregator.collapse(players_list)
            logging.info(f'CIVS GROUPER: Announcing for civ {civ} the results {results}')
            serialized = BatchEncoderDecoder.encode_batch([civ, results])
            # self.channel.basic_publish(exchange='', routing_key=self.output_queue_name, body=serialized)
            RabbitUtils.send_to_queue(self.channel, self.output_queue_name, serialized, headers={'id':self.id_grouper})

        self.persistor.persist("FINISH")

        fin_msg = ApiPacketsEncoder.create_fin_pkt()
        RabbitUtils.send_to_queue(self.channel, self.output_queue_name, fin_msg, headers={'id':self.id_grouper})
        
        self.persistor.wipe()


"""
[APPEND (mongol, id_fila_1), CHECK, APPEND (romanos, id_fila_2), CHECK, APPEND (romanos, id_fila_4)]

{“mongol” : set(ids)} dict[mongol].add(id_fila_2)

Leo una fila OK
Proceso OK
APPEND fila OK
CHECK OK
ACK input OK

"""
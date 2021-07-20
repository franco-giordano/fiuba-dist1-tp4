from common.models.persistor import Persistor
import logging
import json
from common.encoders.batch_encoder_decoder import BatchEncoderDecoder

class CivilizationsGrouper:
    def __init__(self, channel, output_queue_name, aggregator, persistance_file):
        self.channel = channel
        self.output_queue_name = output_queue_name
        self.current_civs = {}
        self.persistance_file = persistance_file
        self.aggregator = aggregator
        self.persistor = Persistor(self.persistance_file)
        
        persisted_state = self.persistor.read() 

        for event in persisted_state:
            if event != "CHECK":
                player = json.loads(event)
                self.add_player(player)
                
    def add_player(self, player):
        civ = player['civ']
        prev_val = self.current_civs.get(civ, None)
        self.current_civs[civ] = self.aggregator.collapse(prev_val, player)

    def add_joined_match(self, joined_match):
        for player in joined_match[1]:
            civ = player['civ']
            prev_val = self.current_civs.get(civ, None)
            self.current_civs[civ] = self.aggregator.collapse(prev_val, player)

            self.persistor.persist(json.dumps(player)) # Agrego APPEND

    def received_sentinel(self):
        logging.info(f'CIVS GROUPER: Flushing all grouped civs')
        self.flush_results()
        logging.info(f'CIV GROUPER: Sending sentinel...')
        sentinel = BatchEncoderDecoder.create_encoded_sentinel()
        self.channel.basic_publish(exchange='', routing_key=self.output_queue_name, body=sentinel)

    def flush_results(self):
        for civ,results in self.current_civs.items():
            logging.info(f'CIVS GROUPER: Announcing for civ {civ} the results {results}')
            serialized = BatchEncoderDecoder.encode_batch([civ, results])
            self.channel.basic_publish(exchange='', routing_key=self.output_queue_name, body=serialized)

"""
[APPEND (mongol, id_fila_1), CHECK, APPEND (romanos, id_fila_2), CHECK, APPEND (romanos, id_fila_4)]

{“mongol” : set(ids)} dict[mongol].add(id_fila_2)

Leo una fila OK
Proceso OK
APPEND fila OK
CHECK OK
ACK input OK

"""
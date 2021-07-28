from common.encoders.obj_encoder_decoder import ObjectEncoderDecoder
from common.encoders.api_pkts_encoder_decoder import ApiPacketsEncoder
from common.utils.rabbit_utils import RabbitUtils
from common.models.persistor import Persistor
import logging


class AccumulatorController:
    def __init__(self, rabbit_ip, input_queue_name, output_queue_name, groupers_amount, accumulator):
        self.input_queue_name = input_queue_name
        self.output_queue_name = output_queue_name
        self.groupers_amount = groupers_amount

        self.connection, self.channel = RabbitUtils.setup_connection_with_channel(
            rabbit_ip)

        # input exchange
        RabbitUtils.setup_input_queue(
            self.channel, self.input_queue_name, self._callback)

        # output queue
        RabbitUtils.setup_queue(self.channel, self.output_queue_name)

        self.final_accumulator = accumulator

        self.data_per_grouper = {key: set() for key in range(groupers_amount)}
        self.grouper_state_machine = {key: 'WAITING_FOR_INICIO'
                                      for key in range(groupers_amount)}

        self.persistors = {key: Persistor(f'/persistance/partial-{key}.txt')
                           for key in range(groupers_amount)}

        # WAITING_FOR_INICIO --inicio--> RECVING_ROWS --fin--> FIN_RECVED
        #    |       ^                  (wipeo si ini)             |
        #    \--fin--^--------------------todos los fin------------/
        #
        # si llega algo sin transicion me quedo en el mismo state

    def run(self):
        logging.info('ACCUMULATOR: Waiting for messages. To exit press CTRL+C')
        try:
            self._reload_persisted_state()
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logging.warning(
                'ACCUMULATOR: ######### Received Ctrl+C! Stopping...')
            self.channel.stop_consuming()
        self.connection.close()

    def _callback(self, ch, method, properties, body):
        data = ObjectEncoderDecoder.decode_bytes(body)
        logging.info(f"ACCUMULATOR: Received data {body}...")
        from_id = properties.headers['id']

        if ApiPacketsEncoder.is_control_pkt(data):
            msg_type = data['msg']
            if msg_type == '[[INICIO]]':
                # limpio memoria
                self.data_per_grouper[from_id] = set()

                # no importa si estoy en WAITING o RECVING
                self.grouper_state_machine[from_id] = 'RECVING_ROWS'

                # wipe persistencia
                self._persistor_for_grouper(from_id).wipe()
                self._persistor_for_grouper(from_id).persist("INICIO")
            elif msg_type == '[[FIN]]':
                if self.grouper_state_machine[from_id] == 'RECVING_ROWS':
                    self.grouper_state_machine[from_id] = 'FIN_RECVED'
                    self._persistor_for_grouper(from_id).persist("FIN")

                if self._recved_all_fins():
                    for partial in self.data_per_grouper.values():
                        for serial_data in partial:
                            data = ObjectEncoderDecoder.decode_str(serial_data)
                            self.final_accumulator.add_partial_data(data)

                    self.final_accumulator.flush_results(
                        self.channel, self.output_queue_name)
                    self._reset_variables()
                    self._wipe_persistance()

        else:   # es un dato del dataset
            # agrega en mem
            serialized_data = ObjectEncoderDecoder.encode_obj_str(data)
            self.data_per_grouper[from_id].add(serialized_data)

            # persisto
            self._persistor_for_grouper(from_id).persist(data)

        RabbitUtils.ack_from_method(self.channel, method)

    def _persistor_for_grouper(self, id_grouper):
        return self.persistors[id_grouper]

    def _recved_all_fins(self):
        count = 0
        for state in self.grouper_state_machine.values():
            count += int(state == 'FIN_RECVED')

        return count == self.groupers_amount

    def _reload_persisted_state(self):
        for from_id, persistor in self.persistors.items():
            prev_data = persistor.read()
            for row in prev_data:
                if row == Persistor.CHECK_GUARD:
                    continue
                msg_type = row.strip()
                if msg_type == 'INICIO':
                    self.data_per_grouper[from_id] = set()
                    self.grouper_state_machine[from_id] = 'RECVING_ROWS'
                elif msg_type == 'FIN':
                    if self.grouper_state_machine[from_id] == 'RECVING_ROWS':
                        self.grouper_state_machine[from_id] = 'FIN_RECVED'
                else:   # es un dato del dataset
                    self.data_per_grouper[from_id].add(msg_type)

        if self._recved_all_fins():
            for partial in self.data_per_grouper.values():
                for serial_data in partial:
                    data = ObjectEncoderDecoder.decode_str(serial_data)
                    self.final_accumulator.add_partial_data(data)

            self.final_accumulator.flush_results(
                self.channel, self.output_queue_name)
            self._reset_variables()
            self._wipe_persistance()

    def _reset_variables(self):
        for k in self.grouper_state_machine:
            self.grouper_state_machine[k] = 'WAITING_FOR_INICIO'
        for k in self.data_per_grouper:
            self.data_per_grouper[k] = set()

    def _wipe_persistance(self):
        for persistor in self.persistors:
            persistor.wipe()

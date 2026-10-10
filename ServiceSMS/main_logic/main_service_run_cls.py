from functools import partial
from confluent_kafka import Consumer,Producer
from ServiceSMS.models.kafka_data_models.kafka_incoming_data import KE_SmsService_IN
from ServiceSMS.models.kafka_data_models.kafka_outgoing_data import KE_Front_Api_Message_Service_OUT


class MainServiceRun:    
    def __init__(self):
        self._redis =           None
        self.redis_cfg =        None
        self.kafka_in_cfg =     None
        self.kafka_out_cfg =    None
        self.service_setting =  None
        self._consumer =        None
        self._producer =        None

    def load_settings_from_cfgs(self,redis_cfg,kafka_in_cfg,kafka_out_cfg,service_setting):
        self.redis_cfg = redis_cfg
        self.kafka_in_cfg = kafka_in_cfg
        self.kafka_out_cfg = kafka_out_cfg
        self.service_setting = service_setting

    def _set_kafka_consumer_cfg(self):
        if self.kafka_in_cfg is None:
            raise Exception("ERROR 2223:MainServiceRun:_set_kafka_consumer_cfg: kafka config for incoming data is None ")
        else:
            self._consumer = Consumer(self.kafka_in_cfg['KAFKA_CONSUMER_CONFIG'])
            self._consumer.subscribe(self.kafka_in_cfg['KAFKA_TOPIC_CONSUMER'])  

    def get_kafka_consumer(self):
        return self._consumer

    def get_KAFKA_WAIT_TIME(self):
        return self.kafka_in_cfg['WAIT_TIME']

    def _set_kafka_producer_cfg(self):
        self._producer = Producer(self.kafka_out_cfg['KAFKA_PRODUCER_CONFIG'])   

    def _produce_message(self,topic_to_send,data_to_send,header_to_send):            
        self._producer.produce(
            topic = topic_to_send,
            value = data_to_send,        
            headers = header_to_send)

    def _produce_message_with_callback(self,topic_to_send,data_to_send,header_to_send,callback_function,callback_log_data):
        coupling_fn_with_data = partial(callback_function,*callback_log_data)
        self._producer.produce(
            topic = topic_to_send,
            value = data_to_send,
            callback=coupling_fn_with_data,        
            headers = header_to_send)

    def send_messages_to_external_services(self,
                                           outcoming_event: KE_Front_Api_Message_Service_OUT, 
                                           kafka_event:str,
                                           callback_function_and_args:tuple=None):
        '''callback_function_and_args = (function_used_in_callback, dict_fields_used_in_callback)
        '''
        if callback_function_and_args is None:
            self._producer.produce(
                    topic = kafka_event,
                    value = outcoming_event.get_data_as_binary(),
                    headers = outcoming_event.get_headers_as_tuple_for_kafka())
        else:

            callback_function, callback_log_details = callback_function_and_args     

            #bound function with args
            coupling_fn_with_data = partial(callback_function,*callback_log_details)

            self._producer.produce(
                topic = kafka_event,
                value = outcoming_event.get_data_as_binary(),
                callback=coupling_fn_with_data,        
                headers = outcoming_event.get_headers_as_tuple_for_kafka())


       
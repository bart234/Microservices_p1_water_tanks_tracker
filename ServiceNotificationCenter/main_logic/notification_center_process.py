import redis
from confluent_kafka import Consumer,Producer
from ServiceNotificationCenter.models.kafka_data_models.kafka_out_data import KE_Contact_message_service_OUT
from ServiceNotificationCenter.models.redis_data_models.redis_data_inout import R_Notification_data_INOUT


class NotificationCenterProcess:
    
    def __init__(self):
        self._redis = None
        self.redis_cfg={}
        self.kafka_in_cfg={}
        self.kafka_out_cfg={}
        self._consumer = None
        self._producer = None

    def load_settings_from_cfgs(self,redis_cfg,kafka_in_cfg,kafka_out_cfg):
        self.redis_cfg = redis_cfg
        self.kafka_in_cfg = kafka_in_cfg
        self.kafka_out_cfg = kafka_out_cfg

    def _set_kafka_consumer_cfg(self):
        self._consumer = Consumer(self.kafka_in_cfg['KAFKA_CONSUMER_CONFIG'])
        self._consumer.subscribe(self.kafka_in_cfg['KAFKA_TOPIC_CONSUMER'])  

    def get_kafka_consumer(self):
        return self._consumer

    def _set_redis_cfg(self):
        self._redis= redis.Redis(host=self.redis_cfg['REDIS_HOST'], port=self.redis_cfg['REDIS_PORT'], db=0) 

    def send_to_redis(self,unique_key:str,basic_service_data: dict, additional_data=dict) -> bool:
        try:
            base_dict = basic_service_data if isinstance(basic_service_data,dict) else basic_service_data.model_dump()

            #add additional data
            new_dict= base_dict | additional_data

            #for data validtaion
            redis_data_model = R_Notification_data_INOUT.from_dict(new_dict)

            print(f"INFO: send_to_redis (data):{new_dict}")

            #dcit to binary
            self._redis.set(unique_key,
                            redis_data_model.to_bytes(),
                            ex=self.redis_cfg['REDIS_TIME_TO_EXPIRE_KEY'])  
            return True
        except Exception as e:
            print(f"ERROR: send_to_redis: {e}")
            return False

    def get_from_redis(self,unique_key:str)->R_Notification_data_INOUT:
        ''' return dict, full or empty'''
        result = self._redis.get(unique_key)
        if result == None or result =={}:
            return None
        else:
            return R_Notification_data_INOUT.from_redis(result)
    
    def _set_kafka_producer_cfg(self):
        self._producer = Producer(self.kafka_out_cfg['KAFKA_PRODUCER_CONFIG'])   

    def _produce_message(self,topic_to_send,data_to_send,header_to_send):            
        self._producer.produce(
            topic = topic_to_send,
            value = data_to_send,        
            headers = header_to_send)
        self._producer.flush()
       
    def send_messages_to_external_services(self,redis_data:R_Notification_data_INOUT,header):
        '''iterate on msg_services which are online,
           save to model with None fields
           enchiched that fields later '''
      
        #get dict with services where to send info 
        additional_data = redis_data.additional_data

        for service,contact in additional_data.items():
            
            #prepare correct topic to send
            topic_to_send = (self.kafka_out_cfg['KAFKA_RETURN_TOPIC_FALLBACK_NOTIF']).replace('topic',service)

            ke_output_obj = KE_Contact_message_service_OUT()            
            ke_output_obj.from_redis_notif_model(redis_data,header)
            ke_output_obj.enrich_data('kafka_topic',topic_to_send)
            ke_output_obj.enrich_data('contact',contact)

            self._produce_message(topic_to_send = topic_to_send,
                                data_to_send = ke_output_obj.get_data_as_binary(),
                                header_to_send = ke_output_obj.get_headers_as_tuple_for_kafka())       
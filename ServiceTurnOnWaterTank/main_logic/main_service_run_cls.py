from functools import partial
from confluent_kafka import Consumer,Producer
import time
v=1.0

class MainServiceRun:    
    def __init__(self,serviceLogic,models_list,**kwargs):
        self.test_mode =        True if 'test_mode' in kwargs.keys() else False 
        self.max_run_time_sec = 5
        self._redis =           None
        self.redis_cfg =        kwargs['redis_cfg'] if 'redis_cfg' in kwargs.keys()else None
        self.kafka_in_cfg =     kwargs['kafka_in_cfg'] if 'kafka_in_cfg' in kwargs.keys()else None
        self.kafka_out_cfg =    kwargs['kafka_out_cfg'] if 'kafka_out_cfg' in kwargs.keys()else None
        self.service_setting =  kwargs['service_setting'] if 'service_setting' in kwargs.keys()else None
        self._consumer =        None
        self._producer =        None
        self.models_list =      models_list
        self._ServiceLogic_cls = serviceLogic(kwargs['service_setting'],
                                              kafka_in_cfg=kwargs['kafka_in_cfg'],
                                              kafka_out_cfg=kwargs['kafka_out_cfg'])

        #set consumer / consumer
        self._set_kafka_consumer_cfg()
        self._set_kafka_producer_cfg()

    def service_main_loop(self):
        #timer for test mode
        end_time = None
        if self.test_mode:
            end_time = time.time() + self.max_run_time_sec

        #get consumer
        consumer_= self.get_kafka_consumer()
        
        print(f"Log: {self.service_setting["service_name"]} service is running")   
        try:
            while True:
                #test mode part
                if end_time is not None and time.time() > end_time:
                    print(f"Log Test Mode: Test time limit ({self.max_run_time_sec}s) reached. Exiting loop.")
                    break
                #
                
                msg=consumer_.poll(self.kafka_in_cfg['WAIT_TIME'])
                if msg is None:
                    continue
                if msg.error():
                    print(f"Error during msg read #2330: {msg.error()}")
                    continue

                #match incoming event with validation model
                for el in self.models_list:
                    if el.TARGET_TOPIC == msg.topic():
                        get_correct_model_instance = el

                #gather data from kafka, validate 
                try:
                    incoming_event = get_correct_model_instance()
                    incoming_event.from_msg(msg)
                except Exception as e:                            
                    print(f"Error during model validation KE_to_model #2332: {e}")

                #put kafka data into user logic - return - dict to send futher
                try:
                    list_with_data_to_send_futher = self.run_service_custom_logic(incoming_event)
                except Exception as e:                            
                    print(f"Error during model validation #2334: {e}")

                #sending another KE
                if len(list_with_data_to_send_futher)>0:                   
                    for struct_data_to_send in list_with_data_to_send_futher:

                        #get validation model based on topic where to send to                                                
                        for el in self.models_list:
                            if el.TARGET_TOPIC == struct_data_to_send['topic']:
                                out_model_instance = el                                
                                print(f"Log topic matched to structure: {el.TARGET_TOPIC} - {struct_data_to_send['topic']}")
                
                        #prepare output KE and validate data
                        try:
                            output_event=out_model_instance()
                            output_event.from_dict(struct_data_to_send['dict_data'])
                        except Exception as e:                            
                            print(f"Error during model validation: model default topic is not found in kafka topic #2344: {e}")

                        #evaluate if callback needed
                        if struct_data_to_send['callback'] == True \
                            and 'callback_function' in struct_data_to_send.keys() \
                            and 'log_details' in struct_data_to_send.keys() :
                            try:
                                self.send_messages_to_external_services(outcoming_event=output_event,
                                                                        kafka_event=struct_data_to_send['topic'],
                                                                        callback_function_and_args=(struct_data_to_send['callback_function'],
                                                                                                    struct_data_to_send['log_details']))   
                            except Exception as e:                            
                                print(f"Error during send_messages_to_external_services with callback #2346: {e}")   
                        else:
                            try:
                                self.send_messages_to_external_services(outcoming_event=output_event,
                                                                        kafka_event=struct_data_to_send['topic'])   
                            except Exception as e:
                                print(f"Error during send_messages_to_external_services without callback #2348: {e}")    
   
        except KeyboardInterrupt:
            print(f"Log: {self.service_setting["service_name"]} service is stoping")
        finally:
            consumer_.close()

    def _set_kafka_consumer_cfg(self):
        if self.kafka_in_cfg is None:
            raise Exception("ERROR 2223:MainServiceRun:_set_kafka_consumer_cfg: kafka config for incoming data is None ")
        else:
            try:            
                self._consumer = Consumer(self.kafka_in_cfg['KAFKA_CONSUMER_CONFIG'])
                self._consumer.subscribe(self.kafka_in_cfg['KAFKA_TOPIC_CONSUMER'])  
                print(f"Log: MainServiceRun:_set_kafka_consumer_cfg: loaded correctly")
            except:
                raise Exception("ERROR 2224:MainServiceRun:_set_kafka_consumer_cfg: kafka config loading error ")

    def get_kafka_consumer(self):
        return self._consumer

    def _set_kafka_producer_cfg(self):
        if self.kafka_out_cfg is None:
            raise Exception("ERROR 2233:MainServiceRun:_set_kafka_producer_cfg: kafka config for outcoming data is None ")
        else:
            try:            
                self._producer = Producer(self.kafka_out_cfg['KAFKA_PRODUCER_CONFIG'])   
                print(f"Log: MainServiceRun:_set_kafka_producer_cfg: loaded correctly")
            except:
                raise Exception("ERROR 2234:MainServiceRun:_set_kafka_producer_cfg: kafka config loading error ")    
        

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
                                           outcoming_event, 
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

    def run_service_custom_logic(self,event_object)->list:
        return self._ServiceLogic_cls.logic(event_object)
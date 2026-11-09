from functools import partial
from confluent_kafka import Consumer,Producer
import time
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker,Session
from ServiceDBtoKafka.main_logic.thread_to_pool import start_producer_background_loop
#v=1.1(producer only)

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
        self._ServiceLogic_cls = serviceLogic(service_setting=self.service_setting)
        self._database_conn_string = kwargs['db_conn_string'] if 'service_setting' in kwargs.keys()else None
        self._db_engine =       None
        self._db_session_obj =  None

        #set consumer / consumer
        self._set_kafka_consumer_cfg()
        self._set_kafka_producer_cfg()

        #set db engine
        self._set_db_engine_and_prepare_session()


    def service_main_loop(self):
        #timer for test mode
        end_time = None
        start_producer_background_loop(self._producer)
        if self.test_mode:
            end_time = time.time() + self.max_run_time_sec
       
        print(f"Log: {self.service_setting["service_name"]} service is running")   
        while True:
            #test mode part
            if end_time is not None and time.time() > end_time:
                print(f"Log Test Mode: Test time limit ({self.max_run_time_sec}s) reached. Exiting loop.")

            #no input data required, service reading notprocessed events from db and conver them to events
            try:
                #process main logic: 
                # process input:    nothing
                # background input: getting data about not processed events from table notification
                # job: getting data about not processed records, setting flag to processed 
                #      getting corr_id for each water_tank and enrich data
                #      prepare data in expected format to send back to create KE                

                time.sleep(self.service_setting['sleep_time'])
                list_with_data_to_send_futher = self.run_service_custom_logic()


            except Exception as e:                            
                print(f"Error during service_main_loop #2334: {e}")
                list_with_data_to_send_futher = []

            #sending another KE
            if len(list_with_data_to_send_futher)>0:
                for struct_data_to_send in list_with_data_to_send_futher:

                    #we expect here only: 
                    # topic:   notification_center   ->  KE_Notification_service_OUT (TARGET_TOPIC = "notification_service")
                    # topic:   everything else       ->  KE_CustomEvent_OUT          (TARGET_TOPIC = "event_template")
                    if struct_data_to_send['topic'] == 'notification_center':
                        out_model_instance = next((model for model in self.models_list if model.TARGET_TOPIC == 'notification_center'), None)
                        print(f"Log [{struct_data_to_send['dict_data']['header']['corr_id']}][{struct_data_to_send['dict_data']['header']['action_id']}][{struct_data_to_send['dict_data']['data']['tank_tag']}]:topic - notification_center - matched to structure: notification_center")

                    else:
                        out_model_instance = next((model for model in self.models_list if model.TARGET_TOPIC == 'event_template'), None)
                        print(f"Log [{struct_data_to_send['dict_data']['header']['corr_id']}][{struct_data_to_send['dict_data']['header']['action_id']}][{struct_data_to_send['dict_data']['data']['tank_tag']}]:topic - {struct_data_to_send['topic']} - matched to structure: event_template")
                            
                    
                    #prepare output KE and validate data
                    try:
                        output_event=out_model_instance()
                        output_event.from_dict(struct_data_to_send['dict_data'])
                    except Exception as e:                            
                        print(f"Error during model validation #2344: model default topic is not found in kafka topic : {e}")

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

   
    


    def get_session(self)->Session:
        try:
            return self._db_session_obj()
        except Exception as e:
            print(f"Error during get_session #2010: {e}")
            raise

    def _set_db_engine_and_prepare_session(self):
        if self._database_conn_string is None:
            print("INFO 2211:MainServiceRun:_set_db_engine_and_prepare_session: db connection string is None ")
        else: 
            try:
                self._db_engine =  create_engine(self._database_conn_string,echo=False)
                self._db_session_obj = sessionmaker(bind=self._db_engine)  
                print(f"Log: MainServiceRun:_set_db_engine: set correctly")
            except:
                raise Exception("ERROR 2212:MainServiceRun:_set_db_engine_and_prepare_session: db engine getting error ")

    def _set_kafka_consumer_cfg(self):
        if self.kafka_in_cfg is None:
            print("INFO 2222:MainServiceRun:_set_kafka_consumer_cfg: kafka config for incoming data is None ")
        else:
            try:            
                self._consumer = Consumer(self.kafka_in_cfg['KAFKA_CONSUMER_CONFIG'])
                self._consumer.subscribe(self.kafka_in_cfg['KAFKA_TOPIC_CONSUMER'])  
                print(f"Log: MainServiceRun:_set_kafka_consumer_cfg: loaded correctly")
            except:
                raise Exception("ERROR 2223:MainServiceRun:_set_kafka_consumer_cfg: kafka config loading error ")

    def get_kafka_consumer(self):
        return self._consumer

    def _set_kafka_producer_cfg(self):
        if self.kafka_out_cfg is None:
            print("INFO 2233:MainServiceRun:_set_kafka_producer_cfg: kafka config for outcoming data is None ")
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
        coupling_fn_with_data = partial(callback_function,**callback_log_data)
        self._producer.produce(
            topic = topic_to_send,
            value = data_to_send,
            on_delivery=coupling_fn_with_data,        
            headers = header_to_send)

    def send_messages_to_external_services(self,
                                           outcoming_event, 
                                           kafka_event:str,
                                           callback_function_and_args:tuple=None):
        '''callback_function_and_args = (function_used_in_callback, dict_fields_used_in_callback)
        '''
        if callback_function_and_args is None:
            print("msg without return")
            self._producer.produce(
                    topic = kafka_event,
                    value = outcoming_event.get_data_as_binary(),
                    headers = outcoming_event.get_headers_as_tuple_for_kafka())
        else:
            callback_function, callback_log_details = callback_function_and_args  
            #bound function with args
            coupling_fn_with_data = partial(callback_function,**callback_log_details)

            self._producer.produce(
                topic = kafka_event,
                value = outcoming_event.get_data_as_binary(),
                headers = outcoming_event.get_headers_as_tuple_for_kafka(),
                on_delivery=coupling_fn_with_data)

    def run_service_custom_logic(self)->list:
        session =       None
        custom_logic =  self._ServiceLogic_cls 
        data_for_kafka = []
        list_of_events = []

        #------- db part --------
        try:
            session = self.get_session()            
            not_proceeded_events =  custom_logic.get_last_n_notprocessed_events(session,self.service_setting['record_limit'])
            data_for_kafka =        custom_logic.prepare_data_for_kafka_from_events(not_proceeded_events)

            #db acton - only
            custom_logic.update_not_processed_to_processed(session,not_proceeded_events)

            #new field corr_id
            data_for_kafka =        custom_logic.enrich_data_for_kafka_with_corr_id(session,data_for_kafka)        

            if self.service_setting['prod_mode']:
                session.commit()
        except Exception as e:
            if session:
                session.rollback()
            data_for_kafka={}            
            print(f"Error during run_service_custom_logic #2133: db part failed {e}")
        finally:
            if session:
                session.close()

        #------- prepare content for events ---------
        
        if len(data_for_kafka)>0:
            try:                
                for _,v in data_for_kafka.items():       
                    #new field action_id (generated)
                    v['action_id'] = custom_logic.generate_action_id()  
                                          
                    list_of_events.append(custom_logic.prepare_event_msg(v))

                    #it create notificatio event only if base_event is on service_setting list
                    if v['kafka_topic'] in self.service_setting['topic_list_for_notification_center']:
                        list_of_events.append(custom_logic.prepare_notification_msg(v))

            except Exception as e:    
                print(f"Error during run_service_custom_logic #2134: context preparation failed {e}")                   
                list_of_events=[]

        return list_of_events
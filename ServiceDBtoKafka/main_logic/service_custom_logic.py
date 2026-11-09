import uuid
from abc import ABC
from ServiceDBtoKafka.main_logic.storage_for_function import delivery_report
from ServiceDBtoKafka.db_access_layer.db_mid_layer import RepositoryNotification_tab,RepositoryMaintenenceData

from sqlalchemy.orm import Session
import datetime
v=1.0

class ServiceLogic(ABC):
    def __init__(self):
        super().__init__()
        
class Service_DBtoKafka(ServiceLogic):
    def __init__(self,service_setting,kafka_in_cfg=None,kafka_out_cfg=None):
        self.kafka_in_cfg = kafka_in_cfg
        self.kafka_out_cfg = kafka_out_cfg
        self.service_setting = service_setting
        print("Service_DBtoKafka -------- class ----------loaded")
        if self.service_setting is not None:
            print(f"service_setting:loaded")
    
    #db_action
    def get_last_n_notprocessed_events(self,session:Session,record_limit=10):
        try:
            db = RepositoryNotification_tab(session)
            return db.select_last_n_notprocessed(limit=record_limit)
        except Exception as e:
            print(f"Error during Service_DBtoKafka:get_last_n_notprocessed_events #2020: {e}")
            raise

    def prepare_data_for_kafka_from_events(self,notprocessed_events)->dict:
        #move db obj to list to futher work with that
        #DEPENDENCY on data_for_kafka - result have to be sorted
        try:
            data_for_kafka = {f"{item.tank_tag}-{item.kafka_msg_group}-{item.field_changed}":
                                {'kafka_topic':     item.kafka_msg_group,
                                'tank_tag':         item.tank_tag,
                                'field_to_update':  item.field_changed,
                                'new_value':        item.new_value} for item in notprocessed_events}      
            return data_for_kafka
        except Exception as e:
            print(f"Error during Service_DBtoKafka:_prepare_data_for_kafka_from_events #2030: {e}")
            raise

    #db_action
    def update_not_processed_to_processed(self,session:Session,notprocessed_events):
        try:
            db = RepositoryNotification_tab(session)
            db.update_not_processed_to_processed(1,[item.id for item in notprocessed_events]) 
        except Exception as e:
            print(f"Error during Service_DBtoKafka:_update_not_processed_to_processed #2040: {e}")
            raise       

    #db_action: corr_id is pulled from db from maitenence_tab'''
    def enrich_data_for_kafka_with_corr_id(self,session:Session,data_for_kafka:dict)->dict:        
        try:
            maintenence_db = RepositoryMaintenenceData(session)
            tank_tag_and_corr_id = maintenence_db.get_corr_id_from_list([v['tank_tag'] for v in data_for_kafka.values()])
            for k,v in data_for_kafka.items():
                v['corr_id']=tank_tag_and_corr_id[v['tank_tag']]
            return data_for_kafka
        except Exception as e:
                print(f"Error during Service_DBtoKafka:_enrich_data_for_kafka_with_corr_id #2050: {e}")
                raise


    def generate_action_id(self):
        try:
            return uuid.uuid4().hex 
        except Exception as e:
            print(f"Error during Service_DBtoKafka:generate_action_id #2060: {e}")
            raise 

    def prepare_event_msg(self,data_for_kafka,log_description=None):
        try:
            TARGET_TOPIC = "notification_service"            
            #preparing return msg - setting have to be like model KE_Notification_service_OUT: 
            #current state: raw assign
            return {    "topic":   TARGET_TOPIC,
            "dict_data":{"header":  {'corr_id':     data_for_kafka['corr_id'],
                                    'action_id':    data_for_kafka['action_id']},
                        "data":    {'kafka_topic':  data_for_kafka['kafka_topic'],
                                    'tank_tag':     data_for_kafka['tank_tag'],
                                    'field_to_update': data_for_kafka['field_to_update'],
                                    'new_value':    data_for_kafka['new_value']}
                        },
            
            "callback":True,
            "callback_function":delivery_report,
            "log_details": {'corr_id':      data_for_kafka['corr_id'],
                            'action_id':    data_for_kafka['action_id'],
                            'tank_tag':     data_for_kafka['tank_tag'],
                            'message': self.service_setting['service_callback']
                            }
            }
        except Exception as e:
            print(f"Error during Service_DBtoKafka:prepare_event_msg #2070: {e}")
            raise 

    def prepare_notification_msg(self,data_for_kafka,log_description=None):
        try:
            #preparing return msg - setting have to be like model KE_Notification_service_OUT: 
            #current state: raw assign
            return {    "topic":   data_for_kafka['kafka_topic'],
            "dict_data":{"header":  {'corr_id':     data_for_kafka['corr_id'],
                                    'action_id':   data_for_kafka['action_id']},
                        "data":    {'kafka_topic': data_for_kafka['kafka_topic'],
                                    'tank_tag':     data_for_kafka['tank_tag'],
                                    'field_to_update': data_for_kafka['field_to_update'],
                                    'new_value':    data_for_kafka['new_value']}
                        },
            
            "callback":True,
            "callback_function":delivery_report,
            "log_details": {'corr_id':      data_for_kafka['corr_id'],
                            'action_id':    data_for_kafka['action_id'],
                            'tank_tag':     data_for_kafka['tank_tag'],
                            'message': self.service_setting['notification_callback']
                            }
            }
        except Exception as e:
            print(f"Error during Service_DBtoKafka:prepare_notification_msg #2080: {e}")
            raise 
               
      

   

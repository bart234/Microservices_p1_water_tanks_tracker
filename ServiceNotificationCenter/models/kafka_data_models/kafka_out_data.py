from pydantic import BaseModel,ValidationError
from datetime import datetime
from typing import Optional
from abc import ABC
from typing import TypeVar,Type,Optional
from ServiceNotificationCenter.models.kafka_data_models.kafka_incoming_template import KafkaIncomingTemplate
import json

#notification_service (data):
#{"kafka_topic": "water_tank_power", "tank_tag": "t1", "field_to_update": "turnOnOff", "new_value": "0", "corr_id": "3bf982504a44418f81823e626d02ef24"}
#{'kafka_topic': 'sms_service',       'tank_tag': 'test_tank_id1', 'field_to_update': 'turnOnOff', 'new_value': '0', 'corr_id': '7aa434933924445a92d91c646bc9d03a'}
# #{'kafka_topic': 'sms_service',       'tank_tag': 'test_tank_id1', 'field_to_update': 'turnOnOff', 'new_value': '0', 'corr_id': '7aa434933924445a92d91c646bc9d03a'}

#models for data validation after load
class _Contact_service_data(BaseModel):
    kafka_topic: str
    base_action: str
    tank_tag: str
    field_to_update:str
    new_value: str
    contact:str

class _Contact_service_header(BaseModel):
    corr_id: str
    action_id: str

class KE_Contact_message_service_OUT(KafkaIncomingTemplate[_Contact_service_header,_Contact_service_data]):
    def __init__(self):
        #header
        self.corr_id = None
        self.action_id = None
        #data
        self.kafka_topic = None
        self.base_action = None  
        self.tank_tag = None
        self.field_to_update = None
        self.new_value = None
        self.contact = None
        super().__init__(_Contact_service_header,_Contact_service_data) 

    def enrich_data(self,field_to_enrich,value_for_field):
        if hasattr(self,field_to_enrich) and \
           hasattr(self.validated_values,field_to_enrich) and \
           field_to_enrich in self.decoded_values.keys():
            
            setattr(self,field_to_enrich,value_for_field)        
            setattr(self.validated_values,field_to_enrich,value_for_field)               
            self.decoded_values[field_to_enrich]=value_for_field
        else:
            print(f"ERROR: attr {field_to_enrich} in  {self.__class__.__name__}")
                     
    def from_redis_notif_model(self,redis_data_model:BaseModel,header):
        ''' function load only what is availible and expected to be load
        redis model (in):
        kafka_topic: str    - will be stored in base_action (passing knownledge about event)
        tank_tag: str       -stay
        field_to_update:str -stay
        new_value: str      -stay
        corr_id:str         - to del
        additional_data: dict[("sms_service":"3453453","email_service":"..@.."] -to del
        '''
        try:
            #header
            self.from_header(header)

            #to_dict
            dict_from_redis = redis_data_model.model_dump()

            #modify incoming data and add missing field        
            dict_from_redis['base_action'] = dict_from_redis['kafka_topic']
            dict_from_redis['kafka_topic'] = "None"  
            dict_from_redis['contact'] = "None"   
            #remove additional data
            dict_from_redis.pop('additional_data',None)
            dict_from_redis.pop('corr_id',None)
            #other field are in redis 


            #save prepared date in decoded 
            self.decoded_values =dict_from_redis.copy()

            #save in validated obj
            self.validated_values =self.data_model.model_validate(self.decoded_values)

            #assign values in main class
            for k,v in self.validated_values.model_dump().items():
                setattr(self,k,v)        

        except  (ValidationError, json.JSONDecodeError, Exception) as e:
            print(f"ERROR: during validation: from_msg: {self.__class__.__name__}:from_data: {e}")
            self.validated_values = None
            self.validated_header = None
        return self
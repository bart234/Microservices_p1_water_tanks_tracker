from pydantic import BaseModel,ValidationError
from datetime import datetime
from typing import Optional
from abc import ABC
from typing import TypeVar,Type,Optional
from .kafka_incoming_template import KafkaIncomingTemplate
import json

#notification_service (data):
#{"kafka_topic": "water_tank_power", "tank_tag": "t1", "field_to_update": "turnOnOff", "new_value": "0", "corr_id": "3bf982504a44418f81823e626d02ef24"}
#{'kafka_topic': 'sms_service',       'tank_tag': 'test_tank_id1', 'field_to_update': 'turnOnOff', 'new_value': '0', 'corr_id': '7aa434933924445a92d91c646bc9d03a'}

#models for data validation after load
class _Notification_service_data(BaseModel):
    kafka_topic: str
    tank_tag: str
    field_to_update:str
    new_value: str
    corr_id:str

class _Notification_service_header(BaseModel):
    corr_id: str
    action_id: str

class KE_Notification_service_IN(KafkaIncomingTemplate[_Notification_service_header,_Notification_service_data]):
    def __init__(self):
        #header
        self.corr_id = None
        self.action_id = None
        #data
        self.kafka_topic = None
        self.tank_tag = None
        self.field_to_update = None
        self.new_value = None
        super().__init__(_Notification_service_header,_Notification_service_data) 


#models for data validation after load
class _Front_api_return_msg_data(BaseModel):
    service_time: datetime 
    tank_tag: str
    action:str

class _Front_api_return_msg_header(BaseModel):
    corr_id: str
    action_id: str

class KE_Front_api_return_msg_IN(KafkaIncomingTemplate[_Front_api_return_msg_header,_Front_api_return_msg_data]):
    def __init__(self):
        #header
        self.corr_id = None
        self.action_id = None
        #data
        self.service_time = None 
        self.tank_tag = None
        self.action = None
        super().__init__(_Front_api_return_msg_header,_Front_api_return_msg_data)

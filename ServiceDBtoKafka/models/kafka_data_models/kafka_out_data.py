from pydantic import BaseModel
from ServiceDBtoKafka.models.kafka_data_models.kafka_incoming_template import KafkaIncomingTemplate

# data egzample: notification_service:
#    "values": {"kafka_topic": "notification_service", "tank_tag": "t1","field_to_update": "turnOnOff","new_value": "0", "corr_id": "00000000000000000000001" },
#    "headers": {"corr_id": "00000000000000000000001", "action_id": "10000000000000000000"}

#models for data validation after load
class _Notification_service_data(BaseModel):
    kafka_topic: str
    tank_tag: str
    field_to_update:str
    new_value: str

class _Notification_service_header(BaseModel):
    corr_id: str
    action_id: str

class KE_Notification_service_OUT(KafkaIncomingTemplate[_Notification_service_header,_Notification_service_data]):
    TARGET_TOPIC = "notification_service"
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


# data egzample: water_tank_power
# "values": {"kafka_topic": "water_tank_power","tank_tag": "t1","field_to_update": "turnOnOff","new_value": "0"}, 
# "headers" : {"corr_id": "00000000000000000000001", "action_id": "10000000000000000000"}

class _CustomEvent_data(BaseModel):
    kafka_topic: str
    tank_tag: str
    field_to_update:str
    new_value: str

class _CustomEvent_header(BaseModel):
    corr_id: str
    action_id: str

class KE_CustomEvent_OUT(KafkaIncomingTemplate[_CustomEvent_header,_CustomEvent_data]):
    #sys
    TARGET_TOPIC = "event_template" #this is template,it  will be filled by data from db, depend whate event was triggered
    def __init__(self):
        #header
        self.corr_id = None
        self.action_id = None
        #data
        self.kafka_topic = None
        self.tank_tag = None
        self.field_to_update = None
        self.new_value = None
        super().__init__(_CustomEvent_header,_CustomEvent_data) 
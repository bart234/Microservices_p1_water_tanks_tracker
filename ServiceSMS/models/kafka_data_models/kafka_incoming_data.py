from pydantic import BaseModel
from .kafka_incoming_template import KafkaIncomingTemplate

# #models for data validation after load
class _SmsService_data(BaseModel):
    kafka_topic: str
    base_action: str
    tank_tag: str
    field_to_update:str
    new_value: str
    contact:str

class _SmsService_header(BaseModel):
    corr_id: str
    action_id: str

class KE_SmsService_IN(KafkaIncomingTemplate[_SmsService_header,_SmsService_data]):
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
        super().__init__(_SmsService_header,_SmsService_data) 
   
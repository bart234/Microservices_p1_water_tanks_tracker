from pydantic import BaseModel,ValidationError
from .kafka_incoming_template import KafkaIncomingTemplate

#models for data validation after load
class _Front_Api_Message_data(BaseModel):
    service_time: str
    tank_tag: str
    action:str
    base_action:str

class _Front_Api_Message_header(BaseModel):
    corr_id: str
    action_id: str

class KE_Front_Api_Message_Service_OUT(KafkaIncomingTemplate[_Front_Api_Message_header,_Front_Api_Message_data]):
    def __init__(self):
        #header
        self.corr_id = None
        self.action_id = None
        #data
        self.service_time = None
        self.tank_tag = None
        self.action = None
        self.base_action = None
        super().__init__(_Front_Api_Message_header,_Front_Api_Message_data) 
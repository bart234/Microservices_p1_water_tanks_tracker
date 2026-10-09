from pydantic import BaseModel,ConfigDict
from .kafka_incoming_template import KafkaIncomingTemplate

#models for data validation after load
class _WaterTankPower_data(BaseModel):
    model_config = ConfigDict(extra='ignore')
    kafka_topic: str
    tank_tag: str
    field_to_update:str
    new_value: str

class _WaterTankPower_header(BaseModel):
    corr_id: str
    action_id: str

class KE_WaterTankPower_IN(KafkaIncomingTemplate[_WaterTankPower_header,_WaterTankPower_data]):
    #sys
    TARGET_TOPIC = "water_tank_power"
    def __init__(self):
        #header
        self.corr_id = None
        self.action_id = None
        #data
        self.kafka_topic = None
        self.tank_tag = None
        self.field_to_update = None
        self.new_value = None
        super().__init__(_WaterTankPower_header,_WaterTankPower_data) 
   
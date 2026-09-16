from pydantic import BaseModel,Field


class WaterTankCreation(BaseModel):
    tank_tag: str | None = Field(max_length=15,default=None)
    name:str = Field(max_length=12)
    capacity:int = Field(ge=1,lt=10000)
    owner:str = Field(max_length=12)

class WaterTankFeatures(BaseModel):
    tank_tag:str
    autofill_service:bool =False
    sms_service: bool =False
    logger_service:bool =False
    water_tank_power: bool = False
    valve_status: bool = False
    details_sms_service_contact: bool = False
    details_email_service_contact: bool = False

    def __eq__(self, other):
        if self.tank_tag == other.tank_tag and self.autofill_service == other.autofill_service and \
            self.sms_service == other.sms_service and self.logger_service == other.logger_service and \
            self.water_tank_power == other.water_tank_power and self.valve_status == other.valve_status and \
            self.details_sms_service_contact == other.details_sms_service_contact and self.details_email_service_contact == other.details_email_service_contact:
            return True
        else:
            return False



class WaterTankOneFeatureStatus(BaseModel):
    feature_name: str
    feature_status: bool    


class WaterTank(BaseModel):
    tank_tag:str    
    name:str
    capacity:int
    owner:str

    def __eq__(self, other):
        if self.tank_tag == other.tank_tag and self.name == other.name and \
            self.capacity == other.capacity and self.owner == other.owner:
            return True
        else:
            return False 
        

class WaterTankStatusReturn(BaseModel):
    tank_tag:str
    status:int

class WaterTankWaterLevelReturn(BaseModel):
    tank_tag:str
    water_level:int

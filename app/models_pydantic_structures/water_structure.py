from pydantic import BaseModel,Field


class WaterTankCreation(BaseModel):
    tank_tag: str | None = Field(max_length=15,default=None)
    name:str = Field(max_length=12)
    capacity:int = Field(ge=1,lt=10000)
    owner:str = Field(max_length=12)

class WaterTankFeatures(BaseModel):
    tank_tag:str
    autofill:bool =False
    sms_service: bool =False
    logger:bool =False

    def __eq__(self, other):
        if self.tank_tag == other.tank_tag and self.autofill == other.autofill and \
            self.sms_service == other.sms_service and self.logger == other.logger:
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
    status:int =0
    valve_status:int = 0

    def __eq__(self, other):
        if self.tank_tag == other.tank_tag and self.name == other.name and \
            self.capacity == other.capacity and self.owner == other.owner and \
            self.status == other.status and self.valve_status == other.valve_status:
            return True
        else:
            return False 
        

class WaterTankStatusReturn(BaseModel):
    tank_tag:str
    status:int

class WaterTankWaterLevelReturn(BaseModel):
    tank_tag:str
    water_level:int

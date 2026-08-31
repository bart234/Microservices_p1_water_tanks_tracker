from sqlalchemy import Column,Integer,String
from app.db_cfg import Base


class db_WaterTanks(Base):
    __tablename__='water_tanks'
    id = Column(Integer, primary_key=True)
    correlation_id = Column(String,unique=True)
    tank_tag= Column(String,unique=True)
    name = Column(String)
    capacity = Column(Integer,default=-1)
    owner = Column(String)
    status=Column(Integer)
    valve_status=Column(Integer)

    def __eq__(self, other):
        if self.tank_tag == other.tank_tag and self.name == other.name and \
            self.capacity == other.capacity and self.owner == other.owner and \
            self.status == other.status and self.valve_status == other.valve_status:
            return True
        else:
            return False
         

class db_TanksFeatures(Base):
    __tablename__="tanks_features"    
    id = Column(Integer, primary_key=True)
    tank_tag = Column(String,unique=True)
    autofill = Column(Integer,default=0)
    sms_service = Column(Integer,default=0)
    logger = Column(Integer,default=0)

    def __eq__(self, other):
        if self.tank_tag == other.tank_tag and self.autofill == other.autofill and \
            self.sms_service == other.sms_service and self.logger == other.logger:
            return True
        else:
            return False

    @staticmethod
    def get_features_list():
        list =db_TanksFeatures.__getattribute__
        return list
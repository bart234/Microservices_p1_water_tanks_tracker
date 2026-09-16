from sqlalchemy import Column,Integer,String
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class WaterTanks_tab(Base):
    __tablename__='water_tanks'
    id = Column(Integer, primary_key=True)
    tank_tag= Column(String,unique=True)
    name = Column(String)
    capacity = Column(Integer,default=-1)
    owner = Column(String)

    def __eq__(self, other):
        if self.tank_tag == other.tank_tag and self.name == other.name and \
            self.capacity == other.capacity and self.owner == other.owner:
            return True
        else:
            return False
         

class TanksFeatures_tab(Base):
    __tablename__="tanks_features"    
    id = Column(Integer, primary_key=True)
    tank_tag = Column(String,unique=True)
    autofill_service = Column(Integer,default=0)
    sms_service = Column(Integer,default=0)
    email_service = Column(Integer,default=0)
    logger_service = Column(Integer,default=0)
    water_tank_power=Column(Integer,default=0)
    valve_status=Column(Integer,default=0)
    details_sms_service_contact=Column(String,default=None)
    details_email_service_contact=Column(String,default=None)

    def __eq__(self, other):
        if self.tank_tag == other.tank_tag and self.autofill_service == other.autofill_service and \
            self.sms_service == other.sms_service and self.logger_service == other.logger_service and \
            self.email_service == other.email_service and \
            self.valve_status == other.valve_status and self.water_tank_power == other.water_tank_power and \
            self.details_email_service_contact == other.details_email_service_contact and self.details_sms_service_contact == other.details_sms_service_contact:
            return True
        else:
            return False

    @staticmethod
    def get_features_list():
        list_ =TanksFeatures_tab.__getattribute__
        return list_
from sqlalchemy import Column,Integer,String,DateTime
from base import Base

class MaintenenceData_tab(Base):
    __tablename__='maitenence_data'
    id = Column(Integer, primary_key=True)
    tank_tag= Column(String,unique=True)
    correlation_id = Column(String,default=None)
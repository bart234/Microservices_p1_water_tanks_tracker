from sqlalchemy import Column,Integer,String,DateTime
from app.db_cfg import Base

class Notification_tab(Base):
    __tablename__='notification'
    id = Column(Integer, primary_key=True)
    tank_tag= Column(String)    #
    kafka_msg_group = Column(String)
    field_changed=Column(String)
    new_value=Column(String)  
    process_flag=Column(Integer)  #-1 pending, 0 working, 1 done
    time_recive=Column(DateTime)
    time_done=Column(DateTime)
    service_data=Column(String,default=None)
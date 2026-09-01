from sqlalchemy import Column,Integer,String,DateTime
from base import Base

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


    def __str__(self):
        return f"id:{self.id} tank_tag: {self.tank_tag} kafka_grp:{self.kafka_msg_group} new_value: {self.new_value} process_flag:{self.process_flag}"


    def to_dict(self):
        return {str(a).split(".")[1]:getattr(self,str(a).split(".")[1]) for a in self.__table__.columns}
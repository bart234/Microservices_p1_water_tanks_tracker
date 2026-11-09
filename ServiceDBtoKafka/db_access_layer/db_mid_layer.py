from abc import ABC
from typing import TypeVar,Type,Optional
from sqlalchemy.orm import Session
from sqlalchemy import select,update,delete

from ServiceDBtoKafka.models.models_data_base_structures.tab_notification import Notification_tab
from ServiceDBtoKafka.models.models_data_base_structures.tab_maintenence import MaintenenceData_tab

T=TypeVar("T")

#[T] - required python > s3.12
class SQLAlchemyRepository[T](ABC):
    def __init__(self,session: Session, model: Type[T]):
        self.model = model
        self.session = session

    def add(self,data:T) -> T:
        new_data = data #object will be updated with id after save
        self.session.add(new_data)
        return new_data

    def get(self,tank_tag:str)->Optional[T]:
        query = select(self.model).where(getattr(self.model,'tank_tag')==tank_tag)
        result = self.session.scalar(query)
        return result    

    def get_specific_attr(self,tank_tag:str,attr_name: str):
        try:
            return getattr(self.get(tank_tag),attr_name)  
        except:
            return None

    def select_all(self)->list[T]:
        query = select(self.model)
        result = self.session.scalars(query).all()
        return result    
    
    def select_all_where(self,**kwargs)->list[T]:
        ''' kwargs expected as: 1th - field name, 2nd - value to filter'''
        filters = [getattr(self.model,key) == value for key,value in kwargs.items()]
        query = select(self.model).where(*filters)
        result = self.session.scalars(query).all()
        return result    

    def update(self,tank_tag:str,attr_to_change: str,new_value)->bool:        
        upd_query = update(self.model).\
                    where(getattr(self.model,'tank_tag')==tank_tag).\
                    values({getattr(self.model,attr_to_change):new_value})
        result = self.session.execute(upd_query)
        return result.rowcount >0

    def update_and_return(self,tank_tag:str,attr_to_change: str,new_value)->Optional[T]:
        if self.update(tank_tag,attr_to_change,new_value) >0:
            return self.get(tank_tag)
        else:
            None

    def delete(self,tank_tag:str)->bool:        
        del_query = delete(self.model).\
                    where(getattr(self.model,'tank_tag')==tank_tag)
        result = self.session.execute(del_query)
        return result.rowcount > 0


class RepositoryNotification_tab(SQLAlchemyRepository[Notification_tab]):
    def __init__(self, session):
        super().__init__(session, Notification_tab)

    def select_last_n_notprocessed(self, limit: int =100) -> list[T]:
        ''' get last n not processed
        DEPENDENCY in function: get_last_n_notprocessed_events - result have to be sorted
        '''        
        query = select(self.model).where(self.model.process_flag == -1).order_by(self.model.id.asc()).limit(limit)
        result = self.session.scalars(query).all()
        return result    

    def update_not_processed_to_processed(self,new_value:int,id_list_to_update:list[int]) -> int:
        query = update(self.model).where(self.model.id.in_(id_list_to_update)).values(process_flag=int(new_value))
        updated_row_count =self.session.execute(query)
        return updated_row_count.rowcount 

class RepositoryMaintenenceData():
    def __init__(self,session: Session):
        self.model = MaintenenceData_tab
        self.session = session

    def add(self,data:MaintenenceData_tab) -> MaintenenceData_tab:
        new_data = data #object will be updated with id after save
        self.session.add(new_data)
        return new_data
    
    def get_corr_id(self,tank_tag:str)-> str | None:
        query = select(self.model).where(getattr(self.model,'tank_tag')==tank_tag)
        result = self.session.scalar(query)
        try:
            return result.correlation_id  
        except:
            return None

    def get_corr_id_from_list(self,tag_list:list[str])->dict[str]:
        query = select(self.model.tank_tag,self.model.correlation_id).where(self.model.tank_tag.in_(tag_list))
        result = self.session.execute(query).all()
        return {row.tank_tag:row.correlation_id for row in result}
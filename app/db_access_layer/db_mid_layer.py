from abc import ABC
from typing import TypeVar,Type,Optional
from app.models_pydantic_structures.water_structure import *
from app.models_data_base_structures.tab_water_structure import *
from app.models_data_base_structures.tab_notification import Notification_tab
from sqlalchemy.orm import Session
from sqlalchemy import select,update,delete

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
        result = self.session.scalars(query)
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

class RepositoryWaterTank(SQLAlchemyRepository[db_WaterTanks]):
    def __init__(self, session):
        super().__init__(session, db_WaterTanks)
 
class RepositoryWaterTankFeatures(SQLAlchemyRepository[db_TanksFeatures]):
    def __init__(self, session):
        super().__init__(session, db_TanksFeatures)

class RepositoryNotification_tab(SQLAlchemyRepository[Notification_tab]):
    def __init__(self, session):
        super().__init__(session, Notification_tab)
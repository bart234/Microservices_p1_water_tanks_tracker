from abc import ABC
from typing import TypeVar,Type,Optional
from ServiceNotificationCenter.models.models_data_base_structures.tab_water_structure import WaterTanks_tab, TanksFeatures_tab
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
    
    def select_all_for_tank(self,tank_tag:str)->T:
        query = select(self.model).where(getattr(self.model,'tank_tag')==tank_tag)
        result = self.session.scalars(query)
        return result.first()

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

class RepositoryWaterTank(SQLAlchemyRepository[WaterTanks_tab]):
    def __init__(self, session):
        super().__init__(session, WaterTanks_tab)
 
class RepositoryWaterTankFeatures(SQLAlchemyRepository[TanksFeatures_tab]):
    def __init__(self, session):
        super().__init__(session, TanksFeatures_tab)

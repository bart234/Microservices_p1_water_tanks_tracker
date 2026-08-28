from app.models_data_base_structures.tab_water_structure import db_WaterTanks,db_TanksFeatures
from app.models_data_structures.water_structure import WaterTank,WaterTankFeatures

class Mapper_WaterTanks:
    @staticmethod
    def dta_to_db(data_in: WaterTank)->db_WaterTanks:
        db_obj=db_WaterTanks(tank_tag=data_in.tank_tag,                          
                            name=data_in.name,
                            capacity=data_in.capacity,
                            owner=data_in.owner,
                            status=data_in.status,
                            valve_status=data_in.valve_status)
        return db_obj

    @staticmethod
    def db_to_dta(data_in:db_WaterTanks)->WaterTank:
        dta_obj=WaterTank(tank_tag=data_in.tank_tag,                          
                        name=data_in.name,
                        capacity=data_in.capacity,
                        owner=data_in.owner,
                        status=data_in.status,
                        valve_status=data_in.valve_status)
        return dta_obj


class Mapper_TankFeatures:
    @staticmethod
    def dta_to_db(data_in: WaterTankFeatures)->db_TanksFeatures:
        db_obj=db_TanksFeatures(tank_tag=data_in.tank_tag,
                                autofill=1 if data_in.autofill else 0,
                                sms_service=1 if data_in.sms_service else 0,
                                logger=1 if data_in.logger else 0)
        return db_obj

    @staticmethod
    def db_to_dta(data_in:db_TanksFeatures)->WaterTankFeatures:
        dta_obj=WaterTankFeatures(tank_tag=data_in.tank_tag,
                                autofill=bool(data_in.autofill),
                                sms_service=bool(data_in.sms_service),
                                logger=bool(data_in.logger))
        return dta_obj


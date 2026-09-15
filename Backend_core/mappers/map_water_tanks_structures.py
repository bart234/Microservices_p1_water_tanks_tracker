from Backend_core.models_data_base_structures.tab_water_structure import db_WaterTanks,db_TanksFeatures
from Backend_core.models_pydantic_structures.water_structure import WaterTank,WaterTankFeatures

class Mapper_WaterTanks:
    @staticmethod
    def dta_to_db(data_in: WaterTank)->db_WaterTanks:
        db_obj=db_WaterTanks(tank_tag=data_in.tank_tag,                          
                            name=data_in.name,
                            capacity=data_in.capacity,
                            owner=data_in.owner)
        return db_obj

    @staticmethod
    def db_to_dta(data_in:db_WaterTanks)->WaterTank:
        dta_obj=WaterTank(tank_tag=data_in.tank_tag,                          
                        name=data_in.name,
                        capacity=data_in.capacity,
                        owner=data_in.owner)
        return dta_obj


class Mapper_TankFeatures:
    @staticmethod
    def dta_to_db(data_in: WaterTankFeatures)->db_TanksFeatures:
        db_obj=db_TanksFeatures(tank_tag=data_in.tank_tag,
                                autofill_service=1 if data_in.autofill_service else 0,
                                sms_service=1 if data_in.sms_service else 0,
                                water_tank_power=1 if data_in.water_tank_power else 0,
                                valve_status=1 if data_in.valve_status else 0,
                                logger_service=1 if data_in.logger_service else 0,
                                details_sms_service_contact=1 if data_in.details_sms_service_contact else 0,
                                details_email_service_contact=1 if data_in.details_email_service_contact else 0,
                                )
        return db_obj

    @staticmethod
    def db_to_dta(data_in:db_TanksFeatures)->WaterTankFeatures:
        dta_obj=WaterTankFeatures(tank_tag=data_in.tank_tag,
                                autofill_service=bool(data_in.autofill_service),
                                sms_service=bool(data_in.sms_service),
                                water_tank_power=bool(data_in.water_tank_power),
                                valve_status=bool(data_in.valve_status),
                                logger_service=bool(data_in.logger_service),
                                details_sms_service_contact=bool(data_in.details_sms_service_contact),
                                details_email_service_contact=bool(data_in.details_email_service_contact),
                                )
        return dta_obj


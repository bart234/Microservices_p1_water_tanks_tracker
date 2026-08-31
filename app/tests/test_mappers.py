from app.mappers.map_water_tanks_structures import Mapper_WaterTanks,Mapper_TankFeatures
from app.models_data_base_structures.tab_water_structure import db_WaterTanks,db_TanksFeatures
from app.models_pydantic_structures.water_structure import WaterTank,WaterTankFeatures


def get_watertank_db_obj()->db_WaterTanks:
    test_tank_id = "test_tank1"
    tank = db_WaterTanks(tank_tag=test_tank_id,
                     name='my_test_tank',
                     capacity=10,
                     owner='admin',
                     status=0,
                     valve_status=0)
    return tank

def get_watertank_dta_obj()->WaterTank:
    test_tank_id = "test_tank1"
    tank = WaterTank(tank_tag=test_tank_id,
                     name='my_test_tank',
                     capacity=10,
                     owner='admin',
                     status=0,
                     valve_status=0)
    return tank

def get_tank_feature_db_obj()->db_TanksFeatures:
    test_tank_id = 'test_tank_tag'
    tank_f = db_TanksFeatures(tank_tag=test_tank_id,
                     autofill=0,
                     sms_service=1,
                     logger=0)
    return tank_f

def get_tank_feature_dta_obj()->WaterTankFeatures:
    test_tank_id = 'test_tank_tag'
    tank_f = WaterTankFeatures(tank_tag=test_tank_id,
                     autofill=False,
                     sms_service=True,
                     logger=False)
    return tank_f

def test_mapper_watertank_dta_to_db():
    dta_wt=get_watertank_dta_obj()
    db_wt=get_watertank_db_obj()
    dta_to_db_result = Mapper_WaterTanks.dta_to_db(dta_wt)
    assert db_wt == dta_to_db_result

def test_mapper_watertank_db_to_dta():
    dta_wt=get_watertank_dta_obj()
    db_wt=get_watertank_db_obj()
    db_to_dta_result = Mapper_WaterTanks.db_to_dta(db_wt)
    assert dta_wt == db_to_dta_result

def test_mapper_watertankfeature_dta_to_db():
    dta_wtf=get_tank_feature_dta_obj()
    db_wtf=get_tank_feature_db_obj()
    db_result = Mapper_TankFeatures.dta_to_db(dta_wtf)
    assert db_wtf == db_result

def test_mapper_watertankfeature_db_to_dta():
    dta_wtf=get_tank_feature_dta_obj()
    db_wtf=get_tank_feature_db_obj()
    dta_result = Mapper_TankFeatures.db_to_dta(db_wtf)
    assert dta_wtf == dta_result

 
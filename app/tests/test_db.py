from app.models_data_base_structures.tab_water_structure import db_WaterTanks,db_TanksFeatures
from sqlalchemy import select


def test_water_tank_creation_save(db_test_session):
    test_tank_id = 'test_tank_tag'
    tank = db_WaterTanks(tank_tag=test_tank_id,
                     name='my_test_tank',
                     capacity=10,
                     owner='admin',
                     status=0,
                     valve_status=0)
    db_test_session.add(tank)
    db_test_session.commit()
    
    query = select(db_WaterTanks).where(db_WaterTanks.tank_tag==test_tank_id)
    result =db_test_session.scalar(query)
    db_test_session.commit()
    assert result.tank_tag==tank.tank_tag
    assert result.name==tank.name
    assert result.capacity==tank.capacity
    assert result.owner==tank.owner
    assert result.status==tank.status
    assert result.valve_status==tank.valve_status

def test_water_tank_features_data_creation_save(db_test_session):
    test_tank_id = 'test_tank_tag'
    tank_f = db_TanksFeatures(tank_tag=test_tank_id,
                     autofill_service=0,
                     sms_service=0,
                     logger_service=0)
    db_test_session.add(tank_f)
    db_test_session.commit()
    
    query = select(db_TanksFeatures).where(db_TanksFeatures.tank_tag==test_tank_id)
    result =db_test_session.scalar(query)
    assert result.tank_tag==tank_f.tank_tag
    assert result.autofill_service==tank_f.autofill_service
    assert result.sms_service==tank_f.sms_service
    assert result.logger_service==tank_f.logger_service

    
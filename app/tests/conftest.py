from fastapi.testclient import TestClient
from app.models_data_base_structures.tab_water_structure import Base
from app.models_data_base_structures.tab_water_structure import db_WaterTanks,db_TanksFeatures
from app.main import app
import pytest
from sqlalchemy.orm import sessionmaker,declarative_base
from sqlalchemy import create_engine

@pytest.fixture
def client():
    with TestClient(app) as c:
        yield c

@pytest.fixture
def test_data_for_api_check(client):    
    #default tank for tests, and it will return it
    response = client.post("/tank/create/",
                        headers={},
                        json={'tank_tag':"test_tank1",'name':'name1','capacity':10,'owner':'bob','status':0}
                        )
    assert response.status_code == 200
    return response.json()

@pytest.fixture
def db_test_session():
    # Base = declarative_base() - we need object which was used in data model
    engine = create_engine('sqlite:///:memory:',echo=False)
    Base.metadata.create_all(engine)
    Session=sessionmaker(bind=engine)
    session=Session()
    yield session
    session.close()

@pytest.fixture()
def add_dummy_wt_to_db(db_test_session):
    test_tank_id = 'test_tank_fixtures'
    tank = db_WaterTanks(tank_tag=test_tank_id,
                    name='my_test_tank',
                    capacity=10,
                    owner='admin',
                    status=0,
                    valve_status=0)
    db_test_session.add(tank)
    db_test_session.commit()
    test_tank_id = 'test_tank_fixtures_2nd'
    tank = db_WaterTanks(tank_tag=test_tank_id,
                    name='my_test_tank_2nd',
                    capacity=2234,
                    owner='admin',
                    status=0,
                    valve_status=0)
    db_test_session.add(tank)
    db_test_session.commit()
    test_tank_id = 'test_tank_fixtures_3rd'
    tank = db_WaterTanks(tank_tag=test_tank_id,
                    name='my_test_tank_2nd',
                    capacity=2234,
                    owner='admin',
                    status=0,
                    valve_status=0)
    db_test_session.add(tank)
    db_test_session.commit()


@pytest.fixture()
def add_dummy_wtf_to_db(db_test_session):
    test_tank_id = 'test_tank_fixtures'
    wtf = db_TanksFeatures(tank_tag=test_tank_id,
                    autofill=0,
                    sms_service=0,
                    logger=0)
    db_test_session.add(wtf)
    db_test_session.commit()

    test_tank_id = 'test_tank_fixtures_2nd'
    wtf2 = db_TanksFeatures(tank_tag=test_tank_id,
                    autofill=0,
                    sms_service=0,
                    logger=0)
    db_test_session.add(wtf2)
    db_test_session.commit()

    test_tank_id = 'test_tank_fixtures_3rd'
    wtf3 = db_TanksFeatures(tank_tag=test_tank_id,
                    autofill=0,
                    sms_service=0,
                    logger=0)
    db_test_session.add(wtf3)
    db_test_session.commit()
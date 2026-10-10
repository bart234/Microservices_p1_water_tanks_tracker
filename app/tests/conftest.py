from fastapi.testclient import TestClient
import pytest
from app.api.routers import tank_path
from fastapi import FastAPI
from app.db_cfg import Base
from app.infrastructure.database import get_db
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool
from app.models_data_base_structures.tab_water_structure import db_TanksFeatures,db_WaterTanks

app_for_test = FastAPI()
app_for_test.include_router(tank_path.router)
engine_test = create_engine('sqlite:///:memory:',echo=True, connect_args={"check_same_thread": False},poolclass=StaticPool)
Session=sessionmaker(bind=engine_test)

     
@pytest.fixture
def session_client_with_db(session_db):
    def new_override_get_db():  
        yield session_db
    app_for_test.dependency_overrides[get_db] = new_override_get_db
    with TestClient(app_for_test) as c:
        yield c

@pytest.fixture
def session_db():
    Base.metadata.create_all(bind=engine_test)    
    db_test_session=Session()
    try:
        yield db_test_session
    finally:
        db_test_session.close()
        Base.metadata.drop_all(bind=engine_test)


@pytest.fixture()
def add_dummy_wt_to_db(session_db):
    test_tank_id = 'test_tank_fixtures'
    tank = db_WaterTanks(tank_tag=test_tank_id,
                    name='my_test_tank',
                    capacity=10,
                    owner='admin',
                    status=0,
                    valve_status=0)
    session_db.add(tank)
    session_db.commit()
    test_tank_id = 'test_tank_fixtures_2nd'
    tank = db_WaterTanks(tank_tag=test_tank_id,
                    name='my_test_tank_2nd',
                    capacity=2234,
                    owner='admin',
                    status=0,
                    valve_status=0)
    session_db.add(tank)
    session_db.commit()
    test_tank_id = 'test_tank_fixtures_3rd'
    tank = db_WaterTanks(tank_tag=test_tank_id,
                    name='my_test_tank_2nd',
                    capacity=2234,
                    owner='admin',
                    status=0,
                    valve_status=0)
    session_db.add(tank)
    session_db.commit()

@pytest.fixture
def data_for_test_created_by_api(session_client_with_db):    
    #default tank for tests, and it will return it
    response = session_client_with_db.post("/tank/create/",
                        headers={},
                        json={'tank_tag':"test_tank1",'name':'name1','capacity':10,'owner':'bob','status':0}
                        )
    assert response.status_code == 200
    return response.json()

@pytest.fixture()
def add_dummy_wtf_to_db(session_db):
    test_tank_id = 'test_tank_fixtures'
    wtf = db_TanksFeatures(tank_tag=test_tank_id,
                    autofill_service=0,
                    sms_service=0,
                    logger_service=0)
    session_db.add(wtf)
    session_db.commit()

    test_tank_id = 'test_tank_fixtures_2nd'
    wtf2 = db_TanksFeatures(tank_tag=test_tank_id,
                    autofill_service=0,
                    sms_service=0,
                    logger_service=0)
    session_db.add(wtf2)
    session_db.commit()

    test_tank_id = 'test_tank_fixtures_3rd'
    wtf3 = db_TanksFeatures(tank_tag=test_tank_id,
                    autofill_service=0,
                    sms_service=0,
                    logger_service=0)
    session_db.add(wtf3)
    session_db.commit()
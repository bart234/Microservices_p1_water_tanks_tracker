from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from fastapi import FastAPI
from fastapi.testclient import TestClient
from Backend_core.api.routers import tank_path
from Backend_core.db_cfg import Base
from Backend_core.infrastructure.database import get_db
from Backend_core.models_data_base_structures.tab_water_structure import db_TanksFeatures,db_WaterTanks
from Backend_core.models_pydantic_structures.water_structure import WaterTankFeatures

def test_create_water_tank(session_client_with_db):
    response = session_client_with_db.post("/tank/create/",
                           headers={},
                           json={
                                "tank_tag":"tank_tag_003",
                                "name": "tank_003",
                                "capacity": 230,
                                  "owner": "owner"
                                }
                            )
    assert response.status_code == 200
    assert response.json()["tank_tag"] ==   'tank_tag_003'
    assert response.json()["name"] ==   'tank_003'
    assert response.json()["capacity"] ==   230
    assert response.json()["owner"] ==   'owner'
    assert response.json()["status"] ==  0

def test_create_water_tank_3(session_client_with_db):
    response = session_client_with_db.post("/tank/create/",
                           headers={},
                           json={
                                "name": "tank_003",
                                "capacity": 230,
                                "owner": "owner"
                                }
                            )
    assert response.status_code == 200
    assert response.json()["name"] ==   'tank_003'
    assert response.json()["capacity"] ==   230
    assert response.json()["owner"] ==   'owner'
    assert response.json()["status"] ==  0
    
def test_showalltanks(session_client_with_db,add_dummy_wt_to_db):
    response =session_client_with_db.get("/tank/showalltanks")
    assert response.status_code ==200
    assert response.json()[0]["tank_tag"] ==   'test_tank_fixtures'
    assert response.json()[0]["name"] ==   'my_test_tank'
    assert response.json()[0]["capacity"] ==   10
    assert response.json()[0]["owner"] ==   'admin'
    assert response.json()[0]["status"] ==  0

def test_showalltanks_2(session_client_with_db,session_db,add_dummy_wt_to_db):
    tank = db_WaterTanks(tank_tag='test_tank_fixtures',
                    name='my_test_tank',
                    capacity=10,
                    owner='admin',
                    status=0,
                    valve_status=0)
    tank2 = db_WaterTanks(tank_tag='test_tank_fixtures_2nd',
                    name='my_test_tank_2nd',
                    capacity=2234,
                    owner='admin',
                    status=0,
                    valve_status=0)
    session_db.commit()

    response =session_client_with_db.get("/tank/showalltanks")
    assert response.status_code ==200
    assert response.json()[0]['tank_tag'] ==  tank.tank_tag
    assert response.json()[0]['name'] ==  tank.name
    assert response.json()[0]['capacity'] ==  tank.capacity
    assert response.json()[0]['owner'] ==  tank.owner
    assert response.json()[0]['status'] ==  tank.status
    assert response.json()[0]['valve_status'] ==  tank.valve_status
    assert response.json()[1]['tank_tag'] ==  tank2.tank_tag
    assert response.json()[1]['name'] ==  tank2.name
    assert response.json()[1]['capacity'] ==  tank2.capacity
    assert response.json()[1]['owner'] ==  tank2.owner
    assert response.json()[1]['status'] ==  tank2.status
    assert response.json()[1]['valve_status'] ==  tank2.valve_status

   
def test_check_if_features_for_tank_were_created(session_client_with_db):
    response = session_client_with_db.post("/tank/create/",
                           headers={},
                           json={
                                "tank_tag":"tank_004",
                                "name": "name_004",
                                "capacity": 230,
                                "owner": "owner"
                                }
                            )
    assert response.status_code == 200
    result =session_client_with_db.get(f"/tank/{"tank_004"}/getallfeatures")
    assert result.status_code ==200    
    result_data=result.json()
    assert result_data["tank_tag"] ==  "tank_004"
    assert result_data["autofill_service"] ==   False
    assert result_data["sms_service"] ==   False
    assert result_data["logger_service"] ==   False

def test_check_switchoffswitchon_for_tank(session_client_with_db):
    response = session_client_with_db.post("/tank/create/",
                            headers={},
                            json={
                                "tank_tag": "tank_005",
                                "name": "name_005",
                                "capacity": 230,
                                "owner": "owner"
                                }
                            )
    assert response.status_code == 200
    new_water_id = response.json()['tank_tag']
    response_get =session_client_with_db.get(f"/tank/{new_water_id}/check_status")
    assert response_get.status_code == 200    
    response_get.json()['status'] = 1
    response_get =session_client_with_db.get(f"/tank/{new_water_id}/check_status")
    assert response_get.status_code == 200    
    response_get.json()['status'] = 0
 
def test_check_switchoffswitchon_feature_name(session_client_with_db,data_for_test_created_by_api):
    test_tank_tag = data_for_test_created_by_api['tank_tag']
    for k in  list(WaterTankFeatures.model_fields.keys())[1:]:
        switched=None
        switched_again=None
        response_feature =session_client_with_db.post(f"/tank/{test_tank_tag}/switch/{k}")
        assert response_feature.status_code ==200 
        assert response_feature.json() ==22222
        switched=response_feature.json()[k] 
        response_feature_again =session_client_with_db.post(f"/tank/{test_tank_tag}/switch/{k}")
        assert response_feature_again.status_code ==200 
        switched_again=response_feature_again.json()[k] 
        assert switched!=switched_again

# def test_check_check_feature_name(client,test_data_for_api_check):
#     test_tank_tag = test_data_for_api_check['tank_tag']
#     response_get =client.post(f"/tank/{test_tank_tag}/getallfeatures")
#     assert response_get.status_code ==200    
#     for k in  list(WaterTankFeatures.model_fields.keys())[1:]:
#         response_feature =client.post(f"/tank/{test_tank_tag}/check/{k}")
#         assert response_feature.status_code ==200 
#         assert response_feature.json()['feature_name'] == k
#         assert response_feature.json()['feature_status'] == False

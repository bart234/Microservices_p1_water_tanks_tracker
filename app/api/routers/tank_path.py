from fastapi import FastAPI,HTTPException,APIRouter,Depends
import uuid
from app.models_data_structures.water_structure import *
from app.models_data_base_structures.tab_water_structure import db_TanksFeatures,db_WaterTanks
from app.models_data_base_structures.tab_notification import Notification_tab
from app.infrastructure.database import get_db
from app.db_access_layer.db_mid_layer import RepositoryWaterTank, RepositoryWaterTankFeatures,\
        RepositoryNotification_tab, SQLAlchemyRepository
from app.mappers.map_water_tanks_structures import Mapper_WaterTanks,Mapper_TankFeatures
from sqlalchemy.orm import Session
import datetime

router = APIRouter(prefix="/tank",tags=['tanks'])
    
# orgins = [
#     "http://localhost:3000"
# ]

# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=orgins,
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"]
# )

WTF_LIST_OF_ATTR_TO_SET= list(WaterTankFeatures.model_fields.keys())[1:]



def switch_specific_attr(repo:SQLAlchemyRepository,tank_tag:str,attr_name: str) ->dict[str,int]:
    ''' switch between 1/0 0/1
    return: {'tank_tag':tank_tag,'old_value':curr_value,'next_value':next_val}'''
    curr_value =repo.get_specific_attr(tank_tag,attr_name)
    if curr_value is None:
            return None
    next_val = 0 if curr_value == 1 else 1
    repo.update(tank_tag,attr_name,next_val)
    #repo.session.commit()
    return {'tank_tag':tank_tag,'old_value':curr_value,'next_value':next_val}


@router.get("/addtwodefaulttank")
def add_two_tanks_and_features(db:Session = Depends(get_db)):
    repo = RepositoryWaterTank(db)
    wt1=db_WaterTanks(tank_tag='test_tank_id1',
                        name='my_test_tank',
                        capacity=10,
                        owner='admin',
                        status=0,
                        valve_status=0)
    result =repo.add(wt1)    
    wt2=db_WaterTanks(tank_tag='22344',
                        name='my_test_tank',
                        capacity=10,
                        owner='admin',
                        status=0,
                        valve_status=0)
    result2=repo.add(wt2)
    db.commit()


@router.get("/showallfeatures",response_model=list[WaterTankFeatures])
def get_show_all_water_containers_features(db:Session = Depends(get_db)):
    repo=RepositoryWaterTankFeatures(db)
    return [Mapper_TankFeatures.db_to_dta(el) for el in repo.select_all()]

@router.get("/showalltanks",response_model=list[WaterTank])
def get_show_all_water_containers(db:Session = Depends(get_db)):
    repo = RepositoryWaterTank(db)    
    return [Mapper_WaterTanks.db_to_dta(el) for el in repo.select_all()]

@router.post("/create",response_model=WaterTank)
def post_create_tank(watertank_creation:WaterTankCreation,db:Session = Depends(get_db)):
    dta_new_tank =WaterTank(tank_tag=str(uuid.uuid4())[0:12] if watertank_creation.tank_tag is None else watertank_creation.tank_tag,
                 name=watertank_creation.name,
                 capacity=watertank_creation.capacity,
                 owner=watertank_creation.owner
                 )  
    try:  
        tank_db_to_add = Mapper_WaterTanks.dta_to_db(dta_new_tank)
        db_wt=RepositoryWaterTank(db)
        to_return =db_wt.add(tank_db_to_add)

        dta_tank_features=WaterTankFeatures(tank_tag=dta_new_tank.tank_tag)
        tank_f_db_to_add =Mapper_TankFeatures.dta_to_db(dta_tank_features)    
        db_wtf=RepositoryWaterTankFeatures(db)        
        saved_wtf = db_wtf.add(tank_f_db_to_add)
                
        db.commit()   
        return Mapper_WaterTanks.db_to_dta(to_return)
    
    except Exception as e:
        db.rollback()
        #TODO: log some error to logs not to user
        raise HTTPException(status_code=400, detail=f"Tank {dta_new_tank.name}: error during save")

@router.get("/{tank_tag}/check_status",response_model=WaterTankStatusReturn)
def get_tank_status(tank_tag:str,db:Session = Depends(get_db)):
    db_wt=RepositoryWaterTank(db)
    value_to_return =db_wt.get_specific_attr(tank_tag,'status')
    if value_to_return is None:
        raise HTTPException(status_code=400, detail=f"Tank: {tank_tag} do not exist")
    return WaterTankStatusReturn(tank_tag=tank_tag, status=value_to_return)

@router.get("/{tank_tag}/getallfeatures",response_model=WaterTankFeatures)
def get_tank_all_details(tank_tag:str,db:Session = Depends(get_db)):
    db_wtf=RepositoryWaterTankFeatures(db)
    wtf_db = db_wtf.get(tank_tag)
    if wtf_db is None:
        raise HTTPException(status_code=400, detail=f"Tank: {tank_tag} do not exist")
    value_to_return = Mapper_TankFeatures.db_to_dta(wtf_db)
    return value_to_return        
    
@router.post("/{tank_tag}/switchoffswitchon",response_model=WaterTankStatusReturn)
def post_tank_turnOff_turnOn(tank_tag:str,db:Session = Depends(get_db)):
    try:
        db_wt=RepositoryWaterTank(db)
        return_dict =switch_specific_attr(db_wt,tank_tag,'status')
        notif = Notification_tab(tank_tag=tank_tag,kafka_msg_group='turnOnOff',field_changed="status",
                                new_value=return_dict['next_value'],process_flag=-1,
                                time_recive=datetime.datetime.now(datetime.timezone.utc),time_done=None)
        db_not = RepositoryNotification_tab(db)
        db_not.add(notif)
        db.commit()
    except:
        db.rollback()
        if return_dict is None:
                    raise HTTPException(status_code=400, detail=f"Tank: {tank_tag} do not exist")
    return WaterTankStatusReturn(tank_tag=return_dict['tank_tag'], status=return_dict['next_value'])

@router.post("/{tank_tag}/switch/{feature_name}",response_model=WaterTankOneFeatureStatus)
def post_feature_turnOff_turnOn(tank_tag:str,feature_name:str,db:Session = Depends(get_db)): 
    if feature_name in WTF_LIST_OF_ATTR_TO_SET: 
        db_wtf = RepositoryWaterTankFeatures(db)
        return_dict = switch_specific_attr(db_wtf,tank_tag,feature_name)  
        db.commit()
        if return_dict is None:
                        raise HTTPException(status_code=400, detail=f"Tank: {tank_tag} do not exist")      
        return WaterTankOneFeatureStatus(feature_name=feature_name,feature_status=bool(return_dict['next_value']))       
    else:
        raise HTTPException(status_code=400, detail=f"Feature: {feature_name} do not exist")

@router.get("/{tank_tag}/check/{feature_name}",response_model=WaterTankOneFeatureStatus)
def get_feature_value_check(tank_tag:str,feature_name:str,db:Session = Depends(get_db)):  
    if feature_name in WTF_LIST_OF_ATTR_TO_SET:
        db_wtf = RepositoryWaterTankFeatures(db)
        result = db_wtf.get_specific_attr(tank_tag,feature_name)
        if result is None:
            raise HTTPException(status_code=400, detail=f"Tank: {tank_tag} do not exist")
        return WaterTankOneFeatureStatus(feature_name=feature_name,feature_status=bool(result))  
    else:
        raise HTTPException(status_code=400, detail=f"Feature: {feature_name} do not exist")


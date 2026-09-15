from Backend_core.db_access_layer.db_mid_layer import RepositoryWaterTankFeatures
from Backend_core.models_data_base_structures.tab_water_structure import db_TanksFeatures
from sqlalchemy import select
import pytest


class TestWaterTankFeatures:
    def test_add_wt(self,db_test_session):
        self.repo = RepositoryWaterTankFeatures(db_test_session)
        test_tank_id = 'tank_2201'
        wtf = db_TanksFeatures(tank_tag=test_tank_id,
                            autofill_service=0,
                            sms_service=0,
                            logger_service=0)
        result_add =self.repo.add(wtf)
        db_test_session.commit()
        assert result_add==wtf

    def test_get(self,db_test_session,add_dummy_wtf_to_db):
        test_tank_id = 'test_tank_fixtures'
        self.repository = RepositoryWaterTankFeatures(db_test_session)
        result = self.repository.get(test_tank_id)
        wtf = db_TanksFeatures(tank_tag=test_tank_id,
                                autofill_service=0,
                                sms_service=0,
                                logger_service=0)
        assert result == wtf

    def test_get_specific_attr(self,db_test_session,add_dummy_wtf_to_db):
        test_tank_id = 'test_tank_fixtures'
        self.repository = RepositoryWaterTankFeatures(db_test_session)
        result =self.repository.get_specific_attr(test_tank_id,'autofill_service')
        assert result == 0

    def test_get_all(self,db_test_session,add_dummy_wtf_to_db):
        self.repository = RepositoryWaterTankFeatures(db_test_session)
        result = self.repository.select_all()
        assert len(result.all())==3

    def test_update(self,db_test_session,add_dummy_wtf_to_db):
        test_tank_id = 'test_tank_fixtures_2nd'
        self.repository = RepositoryWaterTankFeatures(db_test_session)
        result = self.repository.update(test_tank_id,'autofill_service',1)
        db_test_session.commit()
        assert result 

    def test_update_and_return_wt(self,db_test_session,add_dummy_wtf_to_db):
        test_tank_id = 'test_tank_fixtures_2nd'
        self.repository = RepositoryWaterTankFeatures(db_test_session)
        autofill_service_before = (self.repository.get(test_tank_id)).autofill_service
        result = self.repository.update_and_return(test_tank_id,'autofill_service',1)
        db_test_session.commit()
        assert result.tank_tag == test_tank_id
        assert result.autofill_service != autofill_service_before
        assert result.autofill_service == 1

    def test_delete(self,db_test_session,add_dummy_wtf_to_db):
        test_tank_id = 'test_tank_fixtures_3rd'
        self.repository = RepositoryWaterTankFeatures(db_test_session)
        result = self.repository.delete(test_tank_id)
        db_test_session.commit()
        assert result == True
        assert self.repository.get(test_tank_id) == None

 

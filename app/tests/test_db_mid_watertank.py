from app.db_access_layer.db_mid_layer import RepositoryWaterTank
from app.models_data_base_structures.tab_water_structure import db_WaterTanks


class TestWaterTank:
    def test_add_wt(self,db_test_session):
        self.repo = RepositoryWaterTank(db_test_session)
        test_tank_id = 'tank_2201'
        wt = db_WaterTanks(tank_tag=test_tank_id,
                        name='my_test_tank',
                        capacity=10,
                        owner='admin',
                        status=0,
                        valve_status=0)
        result_add =self.repo.add(wt)
        db_test_session.commit()
        assert result_add==wt

    def test_get(self,db_test_session,add_dummy_wt_to_db):
        test_tank_id = 'test_tank_fixtures'
        self.repository = RepositoryWaterTank(db_test_session)
        result = self.repository.get(test_tank_id)
        tank = db_WaterTanks(tank_tag=test_tank_id,
                        name='my_test_tank',
                        capacity=10,
                        owner='admin',
                        status=0,
                        valve_status=0)
        assert result == tank

    def test_get_specific_attr(self,db_test_session,add_dummy_wt_to_db):
        test_tank_id = 'test_tank_fixtures'
        self.repository = RepositoryWaterTank(db_test_session)
        result =self.repository.get_specific_attr(test_tank_id,'owner')
        assert result == 'admin'

    def test_get_all(self,db_test_session,add_dummy_wt_to_db):
        self.repository = RepositoryWaterTank(db_test_session)
        result = self.repository.select_all()
        assert len(result.all())==3

    def test_update(self,db_test_session,add_dummy_wt_to_db):
        test_tank_id = 'test_tank_fixtures_2nd'
        self.repository = RepositoryWaterTank(db_test_session)
        result = self.repository.update(test_tank_id,'capacity',900)
        db_test_session.commit()
        assert result 

    def test_update_and_return_wt(self,db_test_session,add_dummy_wt_to_db):
        test_tank_id = 'test_tank_fixtures_2nd'
        self.repository = RepositoryWaterTank(db_test_session)
        capacity_before = (self.repository.get(test_tank_id)).capacity
        result = self.repository.update_and_return(test_tank_id,'capacity',900)
        db_test_session.commit()
        assert result.tank_tag == test_tank_id
        assert result.capacity != capacity_before
        assert result.capacity == 900

    def test_delete(self,db_test_session,add_dummy_wt_to_db):
        test_tank_id = 'test_tank_fixtures_3rd'
        self.repository = RepositoryWaterTank(db_test_session)
        result = self.repository.delete(test_tank_id)
        db_test_session.commit()
        assert result == True
        assert self.repository.get(test_tank_id) == None

 

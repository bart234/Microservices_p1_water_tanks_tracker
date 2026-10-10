import json,datetime,pytest,os
from ServiceTurnOnWaterTank.tests.utility.fake_kafka_class_template import FakeKafkaClassStructure
from ServiceTurnOnWaterTank.models.kafka_data_models.kafka_incoming_data import KE_WaterTankPower_IN
from ServiceTurnOnWaterTank.models.kafka_data_models.kafka_outgoing_data import KE_Front_Api_Message_Service_OUT

        
class TestKafkaWaterTankPowerServiceModel:        
    def test_water_tank_power_service_KE_correct(self):        
        fake_raw_values = b'{"kafka_topic": "water_tank_power","tank_tag": "t1", "field_to_update": "turnOnOff", "new_value": "0"}'
        fake_raw_header = [('corr_id', b'00000000000000000000004'), ('action_id', b'40000000000000000000')]
        fake_msg = FakeKafkaClassStructure(fake_raw_values,fake_raw_header)

        event = KE_WaterTankPower_IN()
        event.from_msg(fake_msg)

        assert event.validated_header is not None, "SHould be fine"
        assert event.validated_values is not None, "Should be wine"
        
        assert event.kafka_topic == "water_tank_power"
        assert event.tank_tag == "t1"
        assert event.field_to_update == "turnOnOff"
        assert event.new_value == "0"
        assert event.corr_id == "00000000000000000000004"
        assert event.action_id == "40000000000000000000"

    def test_water_tank_power_service_field_missing(self):  
            '''one expected field is missing - fill fail during inner model validation'''      
            fake_raw_values = b'{"kafka_topic": "water_tank_power","tank_tag": "t1","new_value": "0"}'
            fake_raw_header = [('corr_id', b'00000000000000000000004'), ('action_id', b'40000000000000000000')]
            fake_msg = FakeKafkaClassStructure(fake_raw_values,fake_raw_header)
    
            event = KE_WaterTankPower_IN()
            event.from_msg(fake_msg)
    
            assert event.validated_header is None, "expected"
            assert event.validated_values is None, "expected"

        
class TestKafkaFrontApiReturnMsgModel:        
    def test_KE_Front_Api_Message_Service_OUT_KE_correct(self):        
        fake_raw_values = b'{"service_time": "2026-09-17T18:52:36.848608+00:00", "tank_tag": "t1","action": "Water Tank switched OFF","base_action":"water_tank_power"}'
        fake_raw_header = [('corr_id', b'00000000000000000000004'), ('action_id', b'40000000000000000000')]
        fake_msg = FakeKafkaClassStructure(fake_raw_values,fake_raw_header)

        event = KE_Front_Api_Message_Service_OUT()
        event.from_msg(fake_msg)

        assert event.validated_header is not None, "ould be fine"
        assert event.validated_values is not None, "Should be wine"
        
        assert event.service_time ==  '2026-09-17T18:52:36.848608+00:00'
        assert event.tank_tag == "t1"
        assert event.action == "Water Tank switched OFF"
        assert event.base_action == "water_tank_power"
        assert event.corr_id == "00000000000000000000004"
        assert event.action_id == "40000000000000000000"



    def test_KE_Front_Api_Message_Service_OUT_field_missing(self):  
            '''one expected field is missing - fill fail during inner model validation'''      
            fake_raw_values = b'{"service_time": "2026-09-08T08:58:26.211742+00:00", "tank_tag": "t1","action":"switch off"}'
            fake_raw_header = [('corr_id', b'3bf982504a44418f81823e626d02ef24'), ('action_id', b'action_123')]
            fake_msg = FakeKafkaClassStructure(fake_raw_values,fake_raw_header)
    
            event = KE_Front_Api_Message_Service_OUT()
            event.from_msg(fake_msg)
    
            assert event.validated_header is None, "expected"
            assert event.validated_values is None, "expected"

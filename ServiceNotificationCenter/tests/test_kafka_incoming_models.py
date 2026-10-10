import pytest,datetime
from ServiceNotificationCenter.models.kafka_data_models.kafka_incoming_data import KE_Notification_service_IN,KE_Front_api_return_msg_IN

class FakeKafkaIncomingDataNotification:
    def __init__(self,fake_raw_values,fake_raw_header):
        self.fake_raw_values = fake_raw_values
        self.fake_raw_header = fake_raw_header

    def headers(self):
        return self.fake_raw_header

    def value(self):
        return self.fake_raw_values 
    
class FakeKafkaIncomingDataFrontApiReturnMsg:
    def __init__(self,fake_raw_values,fake_raw_header):
        self.fake_raw_values = fake_raw_values
        self.fake_raw_header = fake_raw_header

    def headers(self):
        return self.fake_raw_header

    def value(self):
        return self.fake_raw_values 

        
class TestKafkaNotificationServicegModel:        
    def test_Notification_service_KE_correct(self):        
        fake_raw_values = b'{"kafka_topic": "water_tank_power", "tank_tag": "t1", "field_to_update": "turnOnOff", "new_value": "0", "corr_id": "3bf982504a44418f81823e626d02ef24"}'
        fake_raw_header = [('corr_id', b'3bf982504a44418f81823e626d02ef24'), ('action_id', b'action_123')]
        fake_msg = FakeKafkaIncomingDataNotification(fake_raw_values,fake_raw_header)

        event = KE_Notification_service_IN()
        event.from_msg(fake_msg)

        assert event.validated_header is not None, "SHould be fine"
        assert event.validated_values is not None, "Should be wine"
        
        assert event.kafka_topic == "water_tank_power"
        assert event.tank_tag == "t1"
        assert event.field_to_update == "turnOnOff"
        assert event.new_value == "0"
        assert event.corr_id == "3bf982504a44418f81823e626d02ef24"
        assert event.action_id == "action_123"

    def test_Notification_service_KE_additional_correct(self):
        '''additional field - 'additional_f' field is discarded during inner validation'''        
        fake_raw_values = b'{"kafka_topic": "water_tank_power","additional_f":"22", "tank_tag": "t1", "field_to_update": "turnOnOff", "new_value": "0", "corr_id": "3bf982504a44418f81823e626d02ef24"}'
        fake_raw_header = [('corr_id', b'3bf982504a44418f81823e626d02ef24'), ('action_id', b'action_123')]
        fake_msg = FakeKafkaIncomingDataNotification(fake_raw_values,fake_raw_header)

        event = KE_Notification_service_IN()
        event.from_msg(fake_msg)

        assert event.validated_header is not None, "SHould be fine"
        assert event.validated_values is not None, "Should be wine"
        
        assert event.kafka_topic == "water_tank_power"
        assert event.tank_tag == "t1"
        assert event.field_to_update == "turnOnOff"
        assert event.new_value == "0"
        assert event.corr_id == "3bf982504a44418f81823e626d02ef24"
        assert event.action_id == "action_123"

    def test_Notification_service_field_missing(self):  
            '''one expected field is missing - fill fail during inner model validation'''      
            fake_raw_values = b'{"kafka_topic": "water_tank_power", "field_to_update": "turnOnOff", "new_value": "0", "corr_id": "3bf982504a44418f81823e626d02ef24"}'
            fake_raw_header = [('corr_id', b'3bf982504a44418f81823e626d02ef24'), ('action_id', b'action_123')]
            fake_msg = FakeKafkaIncomingDataNotification(fake_raw_values,fake_raw_header)
    
            event = KE_Notification_service_IN()
            event.from_msg(fake_msg)
    
            assert event.validated_header is None, "expected"
            assert event.validated_values is None, "expected"

        
class TestKafkaFrontApiReturnMsgModel:        
    def test_FrontApiReturnMsg_service_KE_correct(self):        
        fake_raw_values = b'{"service_time": "2026-09-08T08:58:26.211742+00:00", "tank_tag": "t1", "action": "switch on"}'
        fake_raw_header = [('corr_id', b'3bf982504a44418f81823e626d02ef24'), ('action_id', b'action_123')]
        fake_msg = FakeKafkaIncomingDataFrontApiReturnMsg(fake_raw_values,fake_raw_header)

        event = KE_Front_api_return_msg_IN()
        event.from_msg(fake_msg)

        assert event.validated_header is not None, "SHould be fine"
        assert event.validated_values is not None, "Should be wine"
        
        assert event.service_time ==  datetime.datetime(2026, 9, 8, 8, 58, 26, 211742, tzinfo=datetime.timezone.utc)
        assert event.tank_tag == "t1"
        assert event.action == "switch on"
        assert event.corr_id == "3bf982504a44418f81823e626d02ef24"
        assert event.action_id == "action_123"

    def test_FrontApiReturnMsg_service_KE_additional_correct(self):
        '''additional field - 'additional_f field is discarded during inner validation'''        
        fake_raw_values = b'{"service_time": "2026-09-08T08:58:26.211742+00:00", "tank_tag": "t1", "action": "switch on","additional_f":"22"}'
        fake_raw_header = [('corr_id', b'3bf982504a44418f81823e626d02ef24'), ('action_id', b'action_123')]
        fake_msg = FakeKafkaIncomingDataFrontApiReturnMsg(fake_raw_values,fake_raw_header)

        event = KE_Front_api_return_msg_IN()
        event.from_msg(fake_msg)

        assert event.validated_header is not None, "SHould be fine"
        assert event.validated_values is not None, "Should be wine"
        
        assert event.service_time ==  datetime.datetime(2026, 9, 8, 8, 58, 26, 211742, tzinfo=datetime.timezone.utc)
        assert event.tank_tag == "t1"
        assert event.action == "switch on"
        assert event.corr_id == "3bf982504a44418f81823e626d02ef24"
        assert event.action_id == "action_123"

    def test_FrontApiReturnMsg_service_field_missing(self):  
            '''one expected field is missing - fill fail during inner model validation'''      
            fake_raw_values = b'{"service_time": "2026-09-08T08:58:26.211742+00:00", "tank_tag": "t1"}'
            fake_raw_header = [('corr_id', b'3bf982504a44418f81823e626d02ef24'), ('action_id', b'action_123')]
            fake_msg = FakeKafkaIncomingDataFrontApiReturnMsg(fake_raw_values,fake_raw_header)
    
            event = KE_Front_api_return_msg_IN()
            event.from_msg(fake_msg)
    
            assert event.validated_header is None, "expected"
            assert event.validated_values is None, "expected"

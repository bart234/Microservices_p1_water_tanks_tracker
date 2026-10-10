# import json,datetime,pytest,os
# from ServiceSMS.models.kafka_data_models.kafka_incoming_data import KE_SmsService_IN
# from ServiceSMS.models.kafka_data_models.kafka_outgoing_data import KE_Front_Api_Message_Service_OUT
# from ServiceSMS.tests.fake_kafka_class_template import FakeKafkaClassStructure


        
# class TestKafkaSmsServiceModel:        
#     def test_Sms_service_KE_correct(self):        
#         fake_raw_values = b'{"kafka_topic": "sms_service","base_action":"water_tank_power","tank_tag": "t1", "field_to_update": "turnOnOff", "new_value": "0", "contact": "123456987"}'
#         fake_raw_header = [('corr_id', b'3bf982504a44418f81823e626d02ef24'), ('action_id', b'action_123')]
#         fake_msg = FakeKafkaClassStructure(fake_raw_values,fake_raw_header)

#         event = KE_SmsService_IN()
#         event.from_msg(fake_msg)

#         assert event.validated_header is not None, "SHould be fine"
#         assert event.validated_values is not None, "Should be wine"
        
#         assert event.kafka_topic == "sms_service"
#         assert event.base_action == "water_tank_power"
#         assert event.tank_tag == "t1"
#         assert event.field_to_update == "turnOnOff"
#         assert event.new_value == "0"
#         assert event.contact == "123456987"
#         assert event.corr_id == "3bf982504a44418f81823e626d02ef24"
#         assert event.action_id == "action_123"

#     def test_Sms_service_field_missing(self):  
#             '''one expected field is missing - fill fail during inner model validation'''      
#             fake_raw_values = b'{"kafka_topic": "sms_service","base_action":"water_tank_power","tank_tag": "t1","new_value": "0", "contact": "123456987"}'
#             fake_raw_header = [('corr_id', b'3bf982504a44418f81823e626d02ef24'), ('action_id', b'action_123')]
#             fake_msg = FakeKafkaClassStructure(fake_raw_values,fake_raw_header)
    
#             event = KE_SmsService_IN()
#             event.from_msg(fake_msg)
    
#             assert event.validated_header is None, "expected"
#             assert event.validated_values is None, "expected"

        
# class TestKafkaFrontApiReturnMsgModel:        
#     def test_KE_Front_Api_Message_Service_OUT_KE_correct(self):        
#         fake_raw_values = b'{"service_time": "2026-09-08T08:58:26.211742+00:00", "tank_tag": "t1", "action": "switch on","base_action":"water_tank_power"}'
#         fake_raw_header = [('corr_id', b'3bf982504a44418f81823e626d02ef24'), ('action_id', b'action_123')]
#         fake_msg = FakeKafkaClassStructure(fake_raw_values,fake_raw_header)

#         event = KE_Front_Api_Message_Service_OUT()
#         event.from_msg(fake_msg)

#         assert event.validated_header is not None, "SHould be fine"
#         assert event.validated_values is not None, "Should be wine"
        
#         assert event.service_time ==  datetime.datetime(2026, 9, 8, 8, 58, 26, 211742, tzinfo=datetime.timezone.utc)
#         assert event.tank_tag == "t1"
#         assert event.action == "switch on"
#         assert event.base_action == "water_tank_power"
#         assert event.corr_id == "3bf982504a44418f81823e626d02ef24"
#         assert event.action_id == "action_123"



#     def test_KE_Front_Api_Message_Service_OUT_field_missing(self):  
#             '''one expected field is missing - fill fail during inner model validation'''      
#             fake_raw_values = b'{"service_time": "2026-09-08T08:58:26.211742+00:00", "tank_tag": "t1","action":"switch off"}'
#             fake_raw_header = [('corr_id', b'3bf982504a44418f81823e626d02ef24'), ('action_id', b'action_123')]
#             fake_msg = FakeKafkaClassStructure(fake_raw_values,fake_raw_header)
    
#             event = KE_Front_Api_Message_Service_OUT()
#             event.from_msg(fake_msg)
    
#             assert event.validated_header is None, "expected"
#             assert event.validated_values is None, "expected"

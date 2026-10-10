import json,datetime,pytest,os
from ServiceSMS.models.kafka_data_models.kafka_incoming_data import KE_SmsService_IN
from ServiceSMS.models.kafka_data_models.kafka_outgoing_data import KE_Front_Api_Message_Service_OUT
from .fake_kafka_class_template import FakeKafkaClassStructure

test_data_KE_SmsService_IN= os.path.join('ServiceSMS','tests','contract_test_ke_smsservice_in.json')
test_data_KE_Front_api_return_msg_OUT= os.path.join('ServiceSMS','tests','contract_test_ke_front_api_return_msg_out.json')

def from_json_file_to_kafka_data_format(test_data_path:str):
    '''load data from json to dict,and convert these dicts to kafka format'''
    if os.path.exists(test_data_path):
        with open(test_data_path,"r",encoding=('utf-8')) as f:
            contract_data = json.load(f)
        
        #get dicts from file
        file_values =contract_data['values']
        file_header =contract_data['headers']

        #convert dicts to kafka formats
        kafka_values = json.dumps(file_values).encode('utf-8')
        kafka_headers = [(k, v.encode('utf-8')) for k, v in file_header.items()] 
        return kafka_values,kafka_headers
    return None,None 

class TestContractPassingIN:      
    @pytest.fixture()
    def get_test_data_KE_SmsService_IN(self):
        #getting data from  contract test file
        #contract test file will be overriten by result from test before
        return from_json_file_to_kafka_data_format(test_data_KE_SmsService_IN)   

    def test_contract_testing_KE_SmsService_IN(self,get_test_data_KE_SmsService_IN):
        msg_value,msg_header = get_test_data_KE_SmsService_IN
        if msg_value is None or  msg_header is None:
            assert False,"Test data file was not loaded"
        
        fake_kafka_obj = FakeKafkaClassStructure(msg_value,msg_header)
        event_in = KE_SmsService_IN()    
        event_in.from_msg(fake_kafka_obj)      

        dict_msg_value = json.loads(msg_value.decode("utf-8"))    
        dict_msg_header = {k:v.decode("utf-8") for k,v in msg_header } 

        assert event_in.kafka_topic ==  dict_msg_value['kafka_topic']
        assert event_in.base_action == dict_msg_value['base_action']
        assert event_in.tank_tag == dict_msg_value['tank_tag']
        assert event_in.field_to_update == dict_msg_value['field_to_update']
        assert event_in.new_value == dict_msg_value['new_value']
        assert event_in.contact == dict_msg_value['contact']

        assert event_in.corr_id == dict_msg_header['corr_id']
        assert event_in.action_id == dict_msg_header['action_id']


class TestContractPassingOUT:
    def test_contract_testing_KE_Front_api_return_msg_OUT(self):

        fake_raw_values = b'{"service_time": "2026-09-17T18:52:36.848608+00:00", "tank_tag": "t1","action": "sms sent","base_action":"water_tank_power"}'
        fake_raw_header = [('corr_id', b'00000000000000000000004'), ('action_id', b'40000000000000000000')]
        #data to kafka structure
        fake_msg = FakeKafkaClassStructure(fake_raw_values,fake_raw_header)

        #kafka data model and validation
        event_out = KE_Front_Api_Message_Service_OUT()    
        event_out.from_msg(fake_msg)   

        dict_msg_value = json.loads(fake_raw_values.decode("utf-8"))    
        dict_msg_header = {k:v.decode("utf-8") for k,v in fake_raw_header } 
        

        assert event_out.service_time ==  "2026-09-17T18:52:36.848608+00:00"
        assert event_out.tank_tag == dict_msg_value['tank_tag']
        assert event_out.action == dict_msg_value['action']
        assert event_out.base_action == dict_msg_value['base_action']

        assert event_out.corr_id == dict_msg_header['corr_id']
        assert event_out.action_id == dict_msg_header['action_id']

        #dump data to dicts
        contract_data = { "values":event_out.get_data_as_dict(),
                         "headers":event_out.get_headers_as_dict()}
        
        #save as 
        with open(test_data_KE_Front_api_return_msg_OUT,"w", encoding="utf-8") as f:
            json.dump(contract_data,f,indent=4)

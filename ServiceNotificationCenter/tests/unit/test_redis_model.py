import json
from ServiceNotificationCenter.models.redis_data_models.redis_data_inout import R_Notification_data_INOUT

redis_test_data_dict= {'kafka_topic': 'water_tank_power', 
    'tank_tag': 't1', 
    'field_to_update': 'turnOnOff', 
    'new_value': '0', 
    'corr_id': '7369e10591ac447c850f5cfae1223bc4', 
    'additional_data': {"sms_service": "123456789", "email_service": "my_tank1@gmail.com"}}

class TestRedis_IN:
    
    def test_load_model_from_dict(self):
        test_obj = R_Notification_data_INOUT.from_dict(redis_test_data_dict)
        assert test_obj.kafka_topic == redis_test_data_dict['kafka_topic']
        assert test_obj.field_to_update == redis_test_data_dict['field_to_update']
        assert test_obj.new_value == redis_test_data_dict['new_value']
        assert test_obj.corr_id == redis_test_data_dict['corr_id']
        get_add_data =test_obj.additional_data
        for k,v in redis_test_data_dict['additional_data'].items():
            assert get_add_data[k] == v


    def test_load_model_to_dict(self):
        test_obj = R_Notification_data_INOUT.from_dict(redis_test_data_dict)
        dict_from_model = test_obj.to_bytes()
        assert json.loads(dict_from_model.decode("utf-8")) == redis_test_data_dict


    def test_load_bytes_return_model(self):
        dict_to_text = json.dumps(redis_test_data_dict)
        text_to_bytes = dict_to_text.encode("utf-8")
        test_obj = R_Notification_data_INOUT.from_redis(text_to_bytes)
        assert test_obj.kafka_topic == redis_test_data_dict['kafka_topic']
        assert test_obj.field_to_update == redis_test_data_dict['field_to_update']
        assert test_obj.new_value == redis_test_data_dict['new_value']
        assert test_obj.corr_id == redis_test_data_dict['corr_id']
        get_add_data =test_obj.additional_data
        for k,v in redis_test_data_dict['additional_data'].items():
            assert get_add_data[k] == v
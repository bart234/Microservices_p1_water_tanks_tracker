import pytest,json
from unittest.mock import MagicMock, patch
from ServiceNotificationCenter.main_logic.notification_center_process import NotificationCenterProcess
from ServiceNotificationCenter.models.redis_data_models.redis_data_inout import R_Notification_data_INOUT
from ServiceNotificationCenter.settings.kafka_in_out import kafka_out_cfg,kafka_in_cfg

class TestNotificationCenterProcess:
    @pytest.fixture
    def prepare_redis_db(self):
        mock_redis = MagicMock()
        redis_cfg = {'REDIS_TIME_TO_EXPIRE_KEY': 3600}
        return mock_redis, redis_cfg

    def test_send_to_redis(self,prepare_redis_db):
        unique_key = '34534t43t3453456437645'
        data_from_kafka ={'kafka_topic': 'sms_service',
                          'tank_tag': 'test_tank_id1',
                          'field_to_update': 'turnOnOff',
                          'new_value': '0', 
                          'corr_id': '7aa434933924445a92d91c646bc9d03a'}
        additional_data={"sms_service": "123456789", "email_service": "my_tank1@gmail.com"}

        t1 = NotificationCenterProcess()
        mock_redis, redis_cfg = prepare_redis_db
        t1._redis = mock_redis
        t1.redis_cfg = redis_cfg

        result = t1.send_to_redis(unique_key,data_from_kafka,{'additional_data':additional_data})

        assert result is True

        called_args, called_kwargs = t1._redis.set.call_args
        
        assert called_args[0] == unique_key         #key
        assert called_kwargs['ex'] == 3600          #setting

        assert isinstance(called_args[1], bytes)    #data - binary
        assert len(called_args[1]) > 0


       

    def test_send_messages_to_external_services(self,prepare_redis_db):
        r_data_input =  {'kafka_topic': 'water_tank_power', 
                      'tank_tag': 't1', 
                      'field_to_update': 'turnOnOff', 
                      'new_value': '0', 
                      'corr_id': '7369e10591ac447c850f5cfae1223bc4', 
                      'additional_data': {"sms_service": "123456789", "email_service": "my_tank1@gmail.com"}
                      }
        r_data = R_Notification_data_INOUT.from_dict(r_data_input)
        # msg.headers()
        header_data =  [('corr_id', b'3bf982504a44418f81823e626d02ef24'), ('action_id', b'action_123')]

        t1 = NotificationCenterProcess()

        t1._producer = MagicMock()
        t1.kafka_out_cfg = {'KAFKA_RETURN_TOPIC_FALLBACK_NOTIF': 'topic.message'}

        t1.send_messages_to_external_services(r_data,header_data)

        calls = t1._producer.produce.call_args_list

        first_call_kwargs = calls[0].kwargs

        test = json.loads(first_call_kwargs['value'].decode("utf-8"))
        key_ =0
        new_kafka_topic = list(r_data_input['additional_data'].keys())[key_]
        assert test['kafka_topic'] ==  'topic.message'.replace('topic',new_kafka_topic)
        assert test['contact'] ==  list(r_data_input['additional_data'].values())[key_]
        assert test['base_action'] == r_data_input['kafka_topic']
        assert test['tank_tag'] == r_data_input['tank_tag']
        assert test['field_to_update'] == r_data_input['field_to_update']
        assert test['new_value'] == r_data_input['new_value']

        key_ =1
        second_call_kwargs = calls[key_].kwargs
        test = json.loads(second_call_kwargs['value'].decode("utf-8"))    
        new_kafka_topic = list(r_data_input['additional_data'].keys())[key_]    
        assert test['kafka_topic'] ==  'topic.message'.replace('topic',new_kafka_topic)
        assert test['contact'] ==  list(r_data_input['additional_data'].values())[key_]
        assert test['base_action'] == r_data_input['kafka_topic']
        assert test['tank_tag'] == r_data_input['tank_tag']
        assert test['field_to_update'] == r_data_input['field_to_update']
        assert test['new_value'] == r_data_input['new_value']

        
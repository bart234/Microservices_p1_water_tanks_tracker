import pytest, json
from testcontainers.community.kafka import KafkaContainer
from confluent_kafka import Consumer
from models.redis_data_models.redis_data_inout import R_Notification_data_INOUT
from main_logic.notification_center_process import NotificationCenterProcess

@pytest.fixture(scope="session")
def kafka_container():
    #it create kafka instance , it return string with "localhost:54321"
    with KafkaContainer("confluentinc/cp-kafka:7.4.0") as kafka:
        yield kafka.get_bootstrap_server()

class TestNotificationCenterProcess_KafkaInstance:
    def test_send_messages_to_external_services(self,kafka_container):
        #producer is orginal 
        #consumer is test-one

        # ---------------------- general kafka address ------------------
        KAFKA_SERVER_ADDRESS = kafka_container
        #------------------------------------------------------------------

        #-------------------producer creation for class--------------------
        #topic.message - 'topic' will be replaced by expected service like: sms_service.message or email_service.message
        kafka_out_cfg = {'KAFKA_RETURN_TOPIC_FALLBACK_NOTIF':'topic.message',       
                        'KAFKA_PRODUCER_CONFIG' :
                                {'bootstrap.servers':KAFKA_SERVER_ADDRESS}
                        }
        #------------------------------------------------------------------

        #---------------------consumer creation----------------------------
        topics_to_catch = ['sms_service.message']
        group_id = 'NotificationCenter.id'

        kafka_in_cfg = { 'WAIT_TIME': 5.0,      #time between pools
                        'KAFKA_TOPIC_CONSUMER' : topics_to_catch,
                        'KAFKA_CONSUMER_CONFIG' : {
                                                    "bootstrap.servers": KAFKA_SERVER_ADDRESS,
                                                    "group.id": group_id,
                                                    "auto.offset.reset": "earliest"}
                        }
        #------------------------------------------------------------------

        #----------------------data for function --------------------------
        #data setting
        r_data_input =  {'kafka_topic': 'water_tank_power', 
                        'tank_tag': 't1', 
                        'field_to_update': 'turnOnOff', 
                        'new_value': '0', 
                        'corr_id': '7369e10591ac447c850f5cfae1223bc4', 
                        'additional_data': {"sms_service": "123456789", "email_service": "my_tank1@gmail.com"}
                        }
        
        #redis: dict  to redis_model 
        r_data = R_Notification_data_INOUT.from_dict(r_data_input)

        #msg.headers()
        header_data =  [('corr_id', b'3bf982504a44418f81823e626d02ef24'), ('action_id', b'action_123')]
        #------------------------------------------------------------------

        #prepare consumer
        _consumer = Consumer(kafka_in_cfg['KAFKA_CONSUMER_CONFIG'])
        _consumer.subscribe(kafka_in_cfg['KAFKA_TOPIC_CONSUMER'])  

        #configure only what is needed here - kafka producer
        t1 = NotificationCenterProcess()
        t1.kafka_out_cfg=kafka_out_cfg
        t1._set_kafka_producer_cfg()
        t1.send_messages_to_external_services(r_data,header_data)   

        #collect data
        msg = _consumer.poll(timeout=kafka_in_cfg['WAIT_TIME'])

        #assert
        assert msg is not None
        assert msg.error() is None

        extract_data =json.loads(msg.value().decode("utf-8"))
        extract_data['kafka_topic'] = 'sms_service.message'
        extract_data['base_action'] = r_data_input['kafka_topic']
        extract_data['tank_tag'] = r_data_input['tank_tag']
        extract_data['field_to_update'] = r_data_input['field_to_update']
        extract_data['new_value'] = r_data_input['new_value']
        extract_data['contact'] = r_data_input['additional_data']

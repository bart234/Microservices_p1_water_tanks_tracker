import pytest, json,time
from testcontainers.community.kafka import KafkaContainer
from confluent_kafka import Consumer
from ServiceSMS.main_logic.main_service_run_cls import MainServiceRun
from ServiceSMS.models.kafka_data_models.kafka_outgoing_data import KE_Front_Api_Message_Service_OUT

@pytest.fixture(scope="session")
def kafka_container():
    #it create kafka instance , it return string with "localhost:54321"
    with KafkaContainer("confluentinc/cp-kafka:7.4.0") as kafka:
        yield kafka.get_bootstrap_server()

class TestNotificationCenterProcess_KafkaInstance:
    def test_send_messages_to_external_services(self,kafka_container):
        #producer is orginal - it produce KE for fast_api_return_msg
        #consumer is test-one

        # ---------------------- general kafka address ----- ---------------
        KAFKA_SERVER_ADDRESS = kafka_container
        #------------------------------------------------------------------

        #-------------------producer creation for class--------------------
        #topic.message - 'topic' will be replaced by expected service like: sms_service.message or email_service.message
        kafka_out_cfg = {'KAFKA_RETURN_TOPIC_FALLBACK_NOTIF':'front_api_message_service',       
                        'KAFKA_PRODUCER_CONFIG' :
                                {'bootstrap.servers':KAFKA_SERVER_ADDRESS}
                        }
        #------------------------------------------------------------------

        #---------------------consumer creation----------------------------
        topics_to_catch = ['front_api_message_service']
        group_id = 'ServiceSMS.id'

        kafka_in_cfg = { 'WAIT_TIME': 1.0,      #time between pools
                        'KAFKA_TOPIC_CONSUMER' : topics_to_catch,
                        'KAFKA_CONSUMER_CONFIG' : {
                                                    "bootstrap.servers": KAFKA_SERVER_ADDRESS,
                                                    "group.id": group_id,
                                                    "auto.offset.reset": "earliest"}
                        }
        #------------------------------------------------------------------

        #----------------------data for function --------------------------
        #data setting                
        r_data_input =  {'data':{"service_time": "2026-09-17T18:52:36.848608+00:00",
                                "tank_tag": "t1",
                                "action": "sms sent",
                                "base_action":"water_tank_power"}
                           ,
                        'header':{'corr_id':'00000000000000000000004','action_id':'40000000000000000000'}
                        }
        
        #dict to pydantic model
        r_data = KE_Front_Api_Message_Service_OUT()
        r_data.from_dict(r_data_input)

        #------------------------------------------------------------------

        #prepare consumer
        _consumer = Consumer(kafka_in_cfg['KAFKA_CONSUMER_CONFIG'])
        _consumer.subscribe(topics_to_catch) 

        #configure only what is needed here - kafka producer
        t1 = MainServiceRun()
        t1.load_settings_from_cfgs(None,None,kafka_out_cfg,None)
        t1._set_kafka_producer_cfg()
        t1.send_messages_to_external_services(r_data,kafka_out_cfg['KAFKA_RETURN_TOPIC_FALLBACK_NOTIF'])   
        t1._producer.flush()
        
        #collect data
        msg = _consumer.poll(timeout=kafka_in_cfg['WAIT_TIME'])

        #assert
        assert msg is not None
        assert msg.error() is None

        extract_data =json.loads(msg.value().decode("utf-8"))
        assert extract_data['service_time'] ==          "2026-09-17T18:52:36.848608+00:00"
        assert extract_data['tank_tag'] ==              r_data_input['data']['tank_tag']
        assert extract_data['action'] ==                r_data_input['data']['action']
        assert extract_data['base_action'] ==           r_data_input['data']['base_action']
        assert msg.headers()[0][1].decode('utf-8') ==   r_data_input['header']['corr_id']
        assert msg.headers()[1][1].decode('utf-8') ==   r_data_input['header']['action_id']


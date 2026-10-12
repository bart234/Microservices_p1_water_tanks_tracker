import pytest, json,time
from testcontainers.community.kafka import KafkaContainer
from confluent_kafka import Consumer,Producer
from confluent_kafka.admin import AdminClient, NewTopic 
from ServiceTurnOnWaterTank.main_logic.main_service_run_cls import MainServiceRun
from ServiceTurnOnWaterTank.main_logic.service_custom_logic import Service_WaterTankPower
from ServiceTurnOnWaterTank.models.kafka_data_models.kafka_incoming_data import KE_WaterTankPower_IN
from ServiceTurnOnWaterTank.models.kafka_data_models.kafka_outgoing_data import KE_Front_Api_Message_Service_OUT


@pytest.fixture(scope="session")
def kafka_container():
    ''' it create kafka instance , it return string with "localhost:54321"
        also manually create server and with admin lib, topics which are by default added to kafka
        it prevent some errors with:
            KafkaError{code=UNKNOWN_TOPIC_OR_PART,val=3,str="Subscribed topic not available: front_api_return_msg: Broker: Unknown topic or partition"} 
        '''  
    with KafkaContainer("confluentinc/cp-kafka:7.4.0") as kafka:       
        # yield kafka.get_bootstrap_server()

        server_addres = kafka.get_bootstrap_server()
        admin_client = AdminClient({"bootstrap.servers":server_addres})
        topics_to_create = [
            NewTopic("water_tank_power", num_partitions=1, replication_factor=1),
            NewTopic("front_api_return_msg", num_partitions=1, replication_factor=1)       
        ]
        items_to_create = admin_client.create_topics(topics_to_create)
        for topic,f in items_to_create.items():
            try:
                f.result()
            except Exception as e:
                print(f"Failed to create topic manually: {topic}: {e}")
        yield server_addres

class TestE2E_logic_KafkaInstance:
    def test_send_messages_to_external_services(self,kafka_container):
        #1. fake-kafka - produce msg() - cfg
        #2-3.tested function - consume msg (from point 1)
        #2-3.tested funciton - produce msg out (produce msg for point 4)
        #4. fake-kafka - consume msg out(fast_api_return_msg)

        # ---------------------- general kafka address ----- --------------
        KAFKA_SERVER_ADDRESS = kafka_container
        #------------------------------------------------------------------
        
        #----- 4. fake-kafka - consume msg out(fast_api_return_msg) ----------
        fake_end_consumer = Consumer({"bootstrap.servers": KAFKA_SERVER_ADDRESS,
                                      "group.id": 'test_dummy_group.id',
                                      "auto.offset.reset": "earliest"})
        fake_end_consumer.subscribe(['front_api_return_msg']) 
        fake_end_consumer.poll(timeout=0.1)

        #-------------------1. fake-kafka - produce msg() - cfg--------------
        fake_kafka_input_msg = {'KAFKA_TOPIC_PRODUCER':'water_tank_power',       
                                'KAFKA_PRODUCER_CONFIG': {'bootstrap.servers':KAFKA_SERVER_ADDRESS}
                                }
        
        fake_raw_values = b'{"kafka_topic": "water_tank_power","tank_tag": "test1", "field_to_update": "turnOnOff", "new_value": "0"}'
        fake_raw_header = [('corr_id', b'00000000000000000000004'), ('action_id', b'40000000000000000000')]
        fake_start_producer = Producer(fake_kafka_input_msg['KAFKA_PRODUCER_CONFIG'])
        fake_start_producer.produce(topic=fake_kafka_input_msg['KAFKA_TOPIC_PRODUCER'],
                              value=fake_raw_values,
                              headers=fake_raw_header)
        fake_start_producer.flush()



        #-------------------------------cfg-s------------------------------
        #--------------- tested function - consumer msg - cfg--------------
        app_test_kafka_in_cfg = { 'WAIT_TIME': 1.0,      #time between pools
                                'KAFKA_TOPIC_CONSUMER' : ['water_tank_power'],
                                'KAFKA_CONSUMER_CONFIG' : {
                                                            "bootstrap.servers": KAFKA_SERVER_ADDRESS,
                                                            "group.id": 'water_tank_power.id',
                                                            "auto.offset.reset": "earliest"}
                                }
        #-------------- tested funciton - produce msg out - cfg------------
        #topic.message - 'topic' will be replaced by expected service like: sms_service.message or email_service.message
        app_test_kafka_out_cfg = {'KAFKA_TOPIC_PRODUCER':'front_api_return_msg',       
                                 'KAFKA_PRODUCER_CONFIG': {'bootstrap.servers':KAFKA_SERVER_ADDRESS}
                                  }
        #------------------------------------------------------------------
        service_setting = {"service_name": 'Water Tank Power',               #desc
                           "log_description": "Water Tank PowerService-> Sent:"}           #not in use

        
        #------------------------MainServiceRun - logic test---------------
        run = MainServiceRun(Service_WaterTankPower,
                            [KE_WaterTankPower_IN,KE_Front_Api_Message_Service_OUT],
                            kafka_in_cfg=app_test_kafka_in_cfg,
                            kafka_out_cfg=app_test_kafka_out_cfg,
                            service_setting=service_setting,
                            test_mode=True)
        time.sleep(3)
        run.service_main_loop()

        #------------------------------------------------------------------
        
        
        #----------------------- gather data send by logic ----------------
        #collect data
        msg = fake_end_consumer.poll(timeout=5)

        #assert
        assert msg is not None
        assert msg.error() is None

        extract_data =json.loads(msg.value().decode("utf-8"))
        assert extract_data['tank_tag'] ==              'test1'
        assert extract_data['action'] ==                'Water Tank switched OFF'
        assert extract_data['base_action'] ==           'water_tank_power'
        assert msg.headers()[0][1].decode('utf-8') ==   '00000000000000000000004'
        assert msg.headers()[1][1].decode('utf-8') ==   '40000000000000000000'


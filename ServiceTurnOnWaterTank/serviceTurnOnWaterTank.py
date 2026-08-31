from confluent_kafka import Consumer,Producer
import json
import datetime

#to be other consumer for this same msg
#it require diffrent group id 

#data format in #turnOnOff:
# #{'kafka_topic': 'turnOnOff', 'tank_tag': 'test_tank_id1', 'field_to_update': 'status', 'new_value': '1'}

WAIT_TIME = 5
KAFKA_TOPIC_CONSUMER = ['turnOnOff']
KAFKA_GROUP_ID = 'sms_service_id'
KAFKA_BOOTSTRAP_SERVER_CONSUMER ="kafka:29092"
KAFKA_CONSUMER_CONFIG = {
    "bootstrap.servers": "kafka:29092",
    "group.id": 'turnOnOff.waterpump',
    "auto.offset.reset": "earliest"
}
# KAFKA_PRODUCER_CONFIG = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVER_CONSUMER}
# KAFKA_PRODUCER_TOPIC_FALLBACK_NOTIF = 'service_return_msg'

service = 'turnOnOff'                   #topic
service_name = 'turnOnOff WaterTank'   #desc

WAIT_TIME = 5
KAFKA_BOOTSTRAP_SERVER_CONSUMER ="kafka:29092"

#INPUT:
KAFKA_TOPIC_CONSUMER = [service]
KAFKA_CONSUMER_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVER_CONSUMER,
    "group.id": f'turnOnOff.WaterTank',
    "auto.offset.reset": "earliest"}

#OUTPUT:
RETURN_TOPIC_FALLBACK_NOTIF = 'service_return_msg'

#FIELD EXPECTED IN INBOUND:
#tank_tag, new_value , every fielde else will make no issue

#DATAFORMAT IN TOPIC sms_service:
#{'kafka_topic': 'sms_service', 'tank_tag': 'test_tank_id1', 'field_to_update': 'tufnOnOff', 'new_value': '1'}

# def send_feedback_info(producer_config:dict[str,str],kafka_topic:str="service_return_msg",data_to_send_back={}):
#     producer = Producer(producer_config)
#     producer.produce(
#             topic=kafka_topic,
#             value=json.dumps(data_to_send_back).encode("utf-8"))
#     producer.flush()


print(f"Log: {service_name} service is running")
consumer_ = Consumer(KAFKA_CONSUMER_CONFIG)
consumer_.subscribe(KAFKA_TOPIC_CONSUMER)

producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVER_CONSUMER})

try:
    while True:
        msg=consumer_.poll(float(WAIT_TIME))
        if msg is None:
            continue
        if msg.error():
            print(f"Error msg: {msg.error()}")
            continue

        #make action in service 
        in_data= msg.value().decode("utf-8")  
        data = json.loads(in_data)

        msg_back = {'service_time':(datetime.datetime.now(datetime.timezone.utc)).isoformat(),
                    'tank_tag':data['tank_tag'],
                    'action':f'switch {'on' if data['new_value'] =='1' else 'off'}'}
        print(f"Log: {service_name} : {msg_back}")

        #send message back
        producer.produce(topic=RETURN_TOPIC_FALLBACK_NOTIF,
                    value=json.dumps(msg_back).encode("utf-8"))
        
        # send_feedback_info(KAFKA_PRODUCER_CONFIG,
        #                    KAFKA_PRODUCER_TOPIC_FALLBACK_NOTIF,
        #                    msg_back)

except KeyboardInterrupt:
    print(f"Log: {service_name} service is stoping")
finally:
    #we always want ot close that connection
    consumer_.close()
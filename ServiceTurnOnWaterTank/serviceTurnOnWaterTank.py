from functools import partial
from confluent_kafka import Consumer,Producer
import json
import datetime

service = 'turnOnOff'                   #topic
service_name = 'turnOnOff WaterTank'    #desc
callback_desc = "TurnOnOff WT-> API:"


KAFKA_BOOTSTRAP_SERVER_CONSUMER ="kafka:29092"

#INPUT:
WAIT_TIME = 5       #time between pools
KAFKA_TOPIC_CONSUMER = [service]
KAFKA_CONSUMER_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVER_CONSUMER,
    "group.id": f'{service}.WaterTank',
    "auto.offset.reset": "earliest"}


#OUTPUT:
KAFKA_RETURN_TOPIC_FALLBACK_NOTIF = 'service_return_msg'
KAFKA_PRODUCER_CONFIG = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVER_CONSUMER}

#FIELD EXPECTED IN INBOUND:
#date: tank_tag, new_value , every fielde else will make no issue
#header: corr_id in header

#OUTPUT data send by producer:
#{'kafka_topic': 'sms_service', 'tank_tag': 'test_tank_id1', 'field_to_update': 'tufnOnOff', 'new_value': '1'}





def get_corr_id_from_headers(msg)-> str:
    try:
        header_dict ={k: v.decode('utf-8') for k,v in msg.headers()}
        return header_dict.get("corr_id")
    except:
        return None

def delivery_report(err,msg,callback_desc=callback_desc,**kwargs):
    if err: 
        print(f"Log: Delivery error {err}")
    else:           
        v=json.loads(msg.value().decode("utf-8"))
        print(f"Log [{kwargs['corr_id']}][{v['tank_tag']}]: {callback_desc} Delivered",flush=True)

def send_feedback_info(producer_config:dict[str,str],kafka_topic:str,data_to_send_back:dict[str,str],header_content=None,**kwargs):
    producer = Producer(producer_config)
    bound_callback = partial(delivery_report, corr_id=kwargs.get('corr_id'))
    producer.produce(
            topic=kafka_topic,
            value=json.dumps(data_to_send_back).encode("utf-8"),
            callback=bound_callback,
            headers=header_content)
    producer.flush()

def main():
    consumer_ = Consumer(KAFKA_CONSUMER_CONFIG)
    consumer_.subscribe(KAFKA_TOPIC_CONSUMER)   
    print(f"Log: {service_name} service is running")

    try:
        while True:
            msg=consumer_.poll(float(WAIT_TIME))
            if msg is None:
                continue
            if msg.error():
                print(f"Error msg: {msg.error()}")
                continue
                
            #collect data from mgs 
            in_data= msg.value().decode("utf-8")  
            data = json.loads(in_data)
            corr_id=get_corr_id_from_headers(msg)
            tank_tag = data['tank_tag']

            #action to do based on recived data
            try:
                
                pass
                msg_back = {'service_time':(datetime.datetime.now(datetime.timezone.utc)).isoformat(),
                            'tank_tag':tank_tag,
                            'action':f'switch {'on' if data['new_value'] =='1' else 'off'}'}
                

            except Exception as e:
                print(f"ERROR-Log [{corr_id}][{tank_tag}]: {service_name}: {e}")
                    
            #report about action done
            print(f"Log [{corr_id}][{msg_back['tank_tag']}]: {service_name}:  {msg_back}")

            #kafka -> api :report
            send_feedback_info(KAFKA_PRODUCER_CONFIG,
                            KAFKA_RETURN_TOPIC_FALLBACK_NOTIF,
                            msg_back,[("corr_id", corr_id.encode("utf-8"))],
                            corr_id=corr_id)

    except KeyboardInterrupt:
        print(f"Log: {service_name} service is stoping")
    finally:        
        consumer_.close()       #we always want ot close that connection

if __name__ == "__main__":
    main()
#TEMPLATE
from confluent_kafka import Consumer,Producer
import json, datetime
from storage_for_function import get_from_headers,send_feedback_info

service = 'water_tank_power'                   #topic
service_name = 'WaterTank Power'    #desc
callback_desc = "WaterTank Power-> API:"


KAFKA_BOOTSTRAP_SERVER_CONSUMER ="kafka:29092"

#INPUT:
WAIT_TIME = 5       #time between pools
KAFKA_TOPIC_CONSUMER = [service]
KAFKA_CONSUMER_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVER_CONSUMER,
    "group.id": f'{service}.WaterTank',
    "auto.offset.reset": "earliest"}

#OUTPUT:
KAFKA_RETURN_TOPIC_FALLBACK_NOTIF = 'front_api_return_msg'
KAFKA_PRODUCER_CONFIG = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVER_CONSUMER}

#INPUT data expected by consumer:
#date: tank_tag, new_value , every fielde else will make no issue
#header: corr_id, action_id

#OUTPUT data send by producer:
#{'kafka_topic': 'sms_service', 'tank_tag': 'test_tank_id1', 'field_to_update': 'turnOnOff', 'new_value': '1'}



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
            corr_id=get_from_headers(msg,"corr_id")
            action_id=get_from_headers(msg,"action_id")
            tank_tag = data['tank_tag']

            #action to do based on recived data
            try:       
            #----------------------------------------------------------------------------------------------
                msg_back = {'service_time':(datetime.datetime.now(datetime.timezone.utc)).isoformat(),
                            'tank_tag':tank_tag,
                            'action':f'switch {'on' if data['new_value'] =='1' else 'off'}'}
                
            #----------------------------------------------------------------------------------------------
            except Exception as e:
                print(f"ERROR-Log [{corr_id}][{action_id}][{tank_tag}]: {service_name}: {e}")
                    
            #report about action done
            print(f"Log [{corr_id}][{action_id}][{msg_back['tank_tag']}]: {service_name}:  {msg_back}")

            #kafka -> api :report
            send_feedback_info(producer_config=KAFKA_PRODUCER_CONFIG,
                            kafka_topic=KAFKA_RETURN_TOPIC_FALLBACK_NOTIF,
                            data_to_send_back=msg_back,
                            header_content=msg.headers(),
                            callback_desc=callback_desc,
                            corr_id=corr_id,
                            action_id=action_id)

    except KeyboardInterrupt:
        print(f"Log: {service_name} service is stoping")
    finally:        
        consumer_.close()       #we always want ot close that connection

if __name__ == "__main__":
    main()
from functools import partial
from confluent_kafka import Consumer,Producer
import json

def get_from_headers(msg,element_to_get)-> str:
    try:
        header_dict ={k: v.decode('utf-8') for k,v in msg.headers()}
        return header_dict.get(element_to_get)
    except:
        return None
    
def delivery_report(err,msg,**kwargs):
    if err: 
        print(f"Log: Delivery error {err}")
    else:        
        try:
            print(f"Log [{kwargs['corr_id']}][{kwargs['action_id']}][{kwargs['tank_tag']}]: {kwargs['callback_desc']} Delivered",flush=True)
        except:
            print("Log ERROR: cannot parse args")
            
def send_feedback_info(producer_config:dict[str,str],kafka_topic:str,data_to_send_back:dict[str,str],
                       header_content=None,callback_desc=None,corr_id=None,action_id=None):
    producer = Producer(producer_config)
    bound_callback = partial(delivery_report,callback_desc=callback_desc,corr_id=corr_id,action_id=action_id,tank_tag=data_to_send_back['tank_tag'])
    producer.produce(
            topic=kafka_topic,
            value=json.dumps(data_to_send_back).encode("utf-8"),
            callback=bound_callback,
            headers=header_content)
    producer.flush()

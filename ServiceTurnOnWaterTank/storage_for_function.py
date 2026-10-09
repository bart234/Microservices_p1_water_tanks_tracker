from functools import partial
from confluent_kafka import Consumer,Producer
import json
    
def delivery_report(err,msg,**kwargs):
    if err: 
        print(f"Log: Delivery error {err}")
    else:        
        try:
            print(f"Log [{kwargs['corr_id']}][{kwargs['action_id']}][{kwargs['tank_tag']}]: {kwargs['callback_desc']} Delivered",flush=True)
        except:
            print("Log ERROR: cannot parse args")
       
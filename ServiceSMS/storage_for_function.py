import datetime
    
def delivery_report(err,msg,**kwargs):
    ''' kwargs expected: corr_id, action_id, tank_tag, callback_desc'''
    main_msg_part = f"[{kwargs['corr_id']}][{kwargs['action_id']}][{kwargs['tank_tag']}]: {kwargs['callback_desc']}"
    if err: 
        print(f"Log {main_msg_part} Delivery error {err}",flush=True)
    else:        
        try:
            #ts_type:  TIMESTAMP_NOT_AVAILABLE ==0 / TIMESTAMP_CREATE_TIME ==1 / TIMESTAMP_LOG_APPEND_TIME ==2
            #ts_ms #in miliseconds
            ts_type, ts_ms = msg.timestamp()    
            kafka_time_delivery = datetime.datetime.fromtimestamp(ts_ms / 1000.0, tz=datetime.timezone.utc).isoformat()
            msg_size = len(msg)
    
            print(f"Log {main_msg_part} Delivered at {kafka_time_delivery} with size: {msg_size}",flush=True)
        except:
            print("Log ERROR: cannot parse args")
            
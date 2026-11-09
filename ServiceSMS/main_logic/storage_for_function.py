import datetime
    
def delivery_report(err,msg,**kwargs):
    if err: 
        print(f"Log: Delivery error {err}")
    else:      
        corr_id = kwargs.get('corr_id','corr_id missing')
        action_id = kwargs.get('action_id','action_id missing')
        tank_tag = kwargs.get('tank_tag','tank_tag missing')
        message = kwargs.get('message','message is missing')
        print(f"Log [{corr_id}][{action_id}][{tank_tag}]: {message}",flush=True)
            
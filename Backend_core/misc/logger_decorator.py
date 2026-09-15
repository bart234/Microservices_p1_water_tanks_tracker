from misc.logger_service_details import logger_service_events,logger_service_data
import datetime as dt
import json
from dataclasses import asdict
from functools import wraps

#@wraps(func) - it keeps metadata from orginal function

def _extract_function_name(function_name_str):
    try:
        function_name=str(function_name_str).split(" ")[2]
    except:
        function_name=str(function_name_str)
    return function_name

def bus_logger_service(log_type="default"):
    def decorator(base_function):
        @wraps(base_function)
        def wrapper(self, *args, **kwargs):
            if log_type == "default":
                pass
            elif log_type == "event":
                #action before
                logger_service_events.info("Bus:publish: for data  %s  handler: %s",args[1].__name__,_extract_function_name(args[1]))
                #action
                #action after
            elif log_type == "data":
                #self - is a parent class object
                #action before
                event = args[0]
                event_type = type(event)
                one_type_handler = self._handler.get(event_type)
        
                if one_type_handler:
                    for h in one_type_handler:                
                        log_data = {
                            "timestamp": dt.datetime.now().isoformat(),
                            "logger_service": "bus_data_dump",
                            "event_type": event_type.__name__,                     #"WaterRefillingProgres"
                            "handler": _extract_function_name(h),       #"PumpController.water_level_info"
                            "data": asdict(event)                       #{"tank_id": "T12", "wat..
                        }
                        logger_service_data.info(json.dumps(log_data, ensure_ascii=False))
            
            return base_function(self, *args, **kwargs)
        return wrapper
    return decorator           


# def bus_event_logger_service_decorator(base_function):
#     @wraps(base_function)
#     def fn(*args,**kwargs):
#         #action before
#         logger_service_events.info("Bus:publish: for data  %s  handler: %s",args[1].__name__,_extract_function_name(args[2]))
#         #action
#         stg_to_return = base_function(*args,**kwargs)
#         #action after
#         return stg_to_return
#     return fn

# def bus_data_logger_service_decorator(func):
#     @wraps(func)            
#     def wrapper(self, event, *args, **kwargs):
#         #self - is a parent class object
#         #action before
#         event_type = type(event)                        
#         evt_name = event_type.__name__                      
#         one_type_handler = self._handler.get(event_type)

#         if one_type_handler:
#             for h in one_type_handler:                
#                 log_data = {
#                     "timestamp": dt.datetime.now().isoformat(),
#                     "logger_service": "bus_data_dump",
#                     "event_type": evt_name,                     #"WaterRefillingProgres"
#                     "handler": _extract_function_name(h),       #"PumpController.water_level_info"
#                     "data": asdict(event)                       #{"tank_id": "T12", "wat..
#                 }
#                 logger_service_data.info(json.dumps(log_data, ensure_ascii=False))

#         return func(self, event, *args, **kwargs)
#     return wrapper


from pydantic import BaseModel,ValidationError
from abc import ABC
from typing import TypeVar
import json

#redis_data in/out: {'kafka_topic': 'water_tank_power', 
#               'tank_tag': 't1', 
#               'field_to_update': 'turnOnOff', 
#               'new_value': '0', 
#               'corr_id': '7369e10591ac447c850f5cfae1223bc4', 
#               'additional_data: {"sms_service": "123456789", "email_service": "my_tank1@gmail.com"}}'

#---------------------------------------------------------------------

class R_Notification_data_INOUT(BaseModel):
    kafka_topic: str
    tank_tag: str
    field_to_update:str
    new_value: str
    corr_id:str
    additional_data: dict[str,str]

    @staticmethod
    def from_dict(data_dict:dict)-> 'R_Notification_data_INOUT':   
        '''dict to pytantic model'''
        try:     
            return  R_Notification_data_INOUT.model_validate(data_dict)
        except Exception as e:
           print(f"ERROR: R_Notification_data_INOUT: from_dict: {e}")

    @staticmethod
    def from_redis(redis_data:bytes) -> 'R_Notification_data_INOUT':
        '''bytes to pydantic model '''
        try:     
           raw_input_to_decoded_text=redis_data.decode("utf-8")
           decoded_text_to_dict = json.loads(raw_input_to_decoded_text)           
           return  R_Notification_data_INOUT.model_validate(decoded_text_to_dict)
        except Exception as e:
            print(f"ERROR: R_Notification_data_INOUT: from_redis: {e}")


    def to_bytes(self)->bytes:
        '''model to dict - dict to bytes'''
        try:
            return (self.model_dump_json()).encode("utf-8")
        except Exception as e:
            print(f"ERROR: R_Notification_data_INOUT: to_bytes: {e}")




# D = TypeVar("D",bound=BaseModel)

# class RedisDataTemplate[D](ABC):
#     def __init__(self, data_model:type[D]):
#         self.data_model = data_model
#         self.data=None
    
#     def from_dict(self,data_dict:dict):   
#         '''dict to pytantic model'''
#         try:     
#             self.data = self.data_model.model_validate(data_dict)
#             return self.data
#         except Exception as e:
#             print(f"ERROR: R_Notification_data_INOUT: from_dict: {e}")
    
#     def from_redis(self,redis_data:bytes):
#         '''bytes to pydantic model '''
#         try:     
#             raw_input_to_decoded_text=redis_data.decode("utf-8")
#             decoded_text_to_dict = json.loads(raw_input_to_decoded_text)           
#             self.data = self.data_model.model_validate(decoded_text_to_dict)
#             return self.data
#         except Exception as e:
#             print(f"ERROR: R_Notification_data_INOUT: from_redis: {e}")

#     def to_bytes(self)->bytes:
#         '''model to dict - dict to bytes'''
#         try:
#             return (self.data.model_dump_json()).encode("utf-8")
#         except Exception as e:
#             print(f"ERROR: R_Notification_data_INOUT: to_bytes: {e}")

# #---------------------------------------------------------------
# class _Notification_service_data_IN(BaseModel):
#     kafka_topic: str
#     tank_tag: str
#     field_to_update:str
#     new_value: str
#     corr_id:str
#     additional_data: dict[str,str]

# class R_Notification_service_data_IN(RedisDataTemplate[_Notification_service_data_IN]):
#     def __init__(self):
#         super().__init__(_Notification_service_data_IN)


# #---------------------------------------------------------------
# class _Notification_service_data_IN(BaseModel):
#     kafka_topic: str
#     tank_tag: str
#     field_to_update:str
#     new_value: str
#     corr_id:str
#     additional_data: dict[str,str]

# class R_Notification_service_data_IN(RedisDataTemplate[_Notification_service_data_IN]):
#     def __init__(self):
#         super().__init__(_Notification_service_data_IN)

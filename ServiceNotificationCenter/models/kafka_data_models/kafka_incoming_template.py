from pydantic import BaseModel,ValidationError
from abc import ABC
from typing import TypeVar
import json

D = TypeVar("D",bound=BaseModel)
H = TypeVar("H",bound=BaseModel)

class KafkaIncomingTemplate[H,D](ABC):
    '''
    validated_headers/values: return: pydantic_model
    '''
    def __init__(self,header_model:type[H],data_model:type[D]):
        self.data_model = data_model
        self.header_model = header_model
        self.unexpected_fields = {}
        self.raw_values = None
        self.raw_header = None
        self.decoded_header = None
        self.decoded_values = None
        self.validated_header = None
        self.validated_values = None

    def from_msg(self,kafka_msg):  
        try:
            #headers
            self.raw_header = kafka_msg.headers()
            self.decoded_header = {k: v.decode('utf-8') for k,v in self.raw_header}
            self.validated_header = self.header_model.model_validate(self.decoded_header)     #obj pydantic  

            #data
            self.raw_values = kafka_msg.value()
            self.decoded_values = json.loads(self.raw_values.decode("utf-8"))
            self.validated_values =self.data_model.model_validate(self.decoded_values)       #obj pydantic 

            #join dicts
            dict_to_update = {}
            dict_to_update.update(self.validated_header)
            dict_to_update.update(self.validated_values)

            #assign values
            for k,v in dict_to_update.items():
                setattr(self,k,v) 

        except  (ValidationError, json.JSONDecodeError, Exception) as e:
            print(f"ERROR: during validation: from_msg: {self.__class__.__name__}:from_data: {e}")
            self.validated_values = None
            self.validated_header = None

    def from_header(self,kafka_header):  
        '''load header attr, from (expected)obj.headers() - only'''
        try:
            #headers
            self.raw_header = kafka_header
            self.decoded_header = {k: v.decode('utf-8') for k,v in self.raw_header}
            self.validated_header = self.header_model.model_validate(self.decoded_header)     #obj pydantic  

            #join dicts
            dict_to_update = {}
            dict_to_update.update(self.validated_header)

            #assign values
            for k,v in dict_to_update.items():
                setattr(self,k,v) 

        except  (ValidationError, json.JSONDecodeError, Exception) as e:
            print(f"ERROR: during validation: from_msg: {self.__class__.__name__}:from_data: {e}")
            self.validated_header = None

    def get_data(self):
        return self.validated_values

    def get_data_as_binary(self):
        '''model to binary'''
        return (self.validated_values).model_dump_json().encode("utf-8")

    def get_headers(self):
        return self.validated_header
    
    def get_headers_as_tuple_for_kafka(self):
        '''model to list of tuples,second el is binary'''
        header_dict =(self.validated_header).model_dump()  
        return_list = [(k,v.encode("utf-8")) for k,v in header_dict.items()]
        return return_list

    
    def __str__(self):
        dict_to_print = {}
        dict_to_print.update(self.validated_header.model_dump())
        dict_to_print.update(self.validated_values.model_dump())
        return str(dict_to_print)
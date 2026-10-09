import os,json

def from_json_file_to_kafka_data_format(test_data_path:str):
    '''load data from json to dict,and convert these dicts to kafka format'''
    if os.path.exists(test_data_path):
        with open(test_data_path,"r",encoding=('utf-8')) as f:
            contract_data = json.load(f)
        
        #get dicts from file
        file_values =contract_data['values']
        file_header =contract_data['headers']

        #convert dicts to kafka formats
        kafka_values = json.dumps(file_values).encode('utf-8')
        kafka_headers = [(k, v.encode('utf-8')) for k, v in file_header.items()] 
        return kafka_values,kafka_headers
    return None,None 
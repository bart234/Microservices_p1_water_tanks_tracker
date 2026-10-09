from abc import ABC
from ServiceTurnOnWaterTank.storage_for_function import delivery_report
import datetime
v=1.0

class ServiceLogic(ABC):
    def __init__(self):
        super().__init__()
        
class Service_WaterTankPower(ServiceLogic):
    def __init__(self,service_setting,kafka_in_cfg=None,kafka_out_cfg=None):
        self.kafka_in_cfg = kafka_in_cfg
        self.kafka_out_cfg = kafka_out_cfg
        self.service_setting = service_setting
        print("Service_WaterTankPower -------- class ----------loaded")

    def logic(self,event_object):
        '''
        #data input: models:kafka_data_models
        #data return format expected:
        #[{ "topic":"send_out",
        #   "dict_data":{"header":{'corr_id':incoming_event.corr_id,'action_id':incoming_event.action_id},
        #                  "data":{'service_time':(datetime.datetime.now(datetime.timezone.utc)).isoformat(),
        #                          'tank_tag':incoming_event.tank_tag,
        #                          'action':str(dict_with_data),
        #                          'base_action': "SMS Sent"}
        #                },
        #    
        #    "callback":True,
        #    "callback_function":fn_,
        #    "log_details": {'corr_id':incoming_event.corr_id,
        #                    'action_id':incoming_event.action_id,
        #                    'tank_tag':incoming_event.tank_tag,
        #                    'log_description': run.service_setting['log_description']}
        #   }]
        # '''
        print("Log: Start WATER TANK POWER - logic")

        if event_object.TARGET_TOPIC == "water_tank_power":
            print(f"Log:[{event_object.corr_id}] [{event_object.action_id}]: topic: water_tank_power") 

            #main action for that topic
            print(f"Log:[{event_object.corr_id}] [{event_object.action_id}]: action: water tank power switched: set to state {'OFF' if event_object.new_value == '0' else 'ON'}")


            #preparing return msg 
            return [{ "topic":self.kafka_out_cfg['KAFKA_TOPIC_PRODUCER'],
            "dict_data":{"header":{'corr_id':event_object.corr_id,'action_id':event_object.action_id},
                            "data":{'service_time':(datetime.datetime.now(datetime.timezone.utc)).isoformat(),
                                    'tank_tag':event_object.tank_tag,
                                    'action':  f"Water Tank switched {'OFF' if event_object.new_value == '0' else 'ON'}",
                                    'base_action': 'water_tank_power'}
                        },
            
            "callback":True,
            "callback_function":delivery_report,
            "log_details": {'corr_id':event_object.corr_id,
                            'action_id':event_object.action_id,
                            'tank_tag':event_object.tank_tag,
                            'log_description': self.service_setting['log_description']}
            }]
        else:
            return []



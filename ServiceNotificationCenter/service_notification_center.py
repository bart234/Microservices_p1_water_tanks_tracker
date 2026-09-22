from ServiceNotificationCenter.infrastructure.db_cfg import get_db
from ServiceNotificationCenter.models.kafka_data_models.kafka_incoming_data import KE_Front_api_return_msg_IN,KE_Notification_service_IN
from ServiceNotificationCenter.db_access_layer.db_mid_layer import RepositoryWaterTankFeatures
from ServiceNotificationCenter.settings.redis_cfg import redis_cfg
from ServiceNotificationCenter.settings.kafka_in_out import kafka_in_cfg,kafka_out_cfg,KAFKA_INCOMING_TOPICS
from ServiceNotificationCenter.settings.service_setting import service_name
from ServiceNotificationCenter.infrastructure.db_cfg import get_db
from ServiceNotificationCenter.main_logic.notification_center_process import NotificationCenterProcess
NOTIFICATION_SERVICE = KAFKA_INCOMING_TOPICS[0]
FRONT_API_RETURN_MSG = KAFKA_INCOMING_TOPICS[1]

             
            
def main():     
    '''
    communication: clean services -> service_notification_center -> sms/email services
    why: system to generate sms/emails if service is online
    1. done get kafka event notification_service (event sent if some action is goting to be do)- 
       done         check some message service is online for that tank_id (check in db) 
       done         add to redis: corr_is+action_id:notification_service.data(from_notification_service_msg)
                if is not online, nothing will happen - loop end
    2. get kafka event front_api_return_msg - 
                check if that corr_id+action_id is in redis - if not, nothing will hapen - loop end
    3. retrive data from redis - here we are sure that event was sent todo something, and it did something, and have detailes data about from redis
    4. ask db about details for that sms/email service - number / @
    5. prepare new topic notificaiton.send_sms / notificaiton.send_email, prepare msg , to send, keep header in 
    6. no callback
    TODO: rrename return topis in api to avoid beeing catch by that service, add that topic to front catcher 
    
    '''

    run= NotificationCenterProcess()
    run.load_settings_from_cfgs(redis_cfg,kafka_in_cfg,kafka_out_cfg)
    run._set_kafka_consumer_cfg()
    run._set_redis_cfg()
    run._set_kafka_producer_cfg()

    print(f"Log: {service_name} service is running")   
    # try:
    while True:

        msg = run.get_kafka_consumer().poll(1.0)    
        if msg is None:
            continue
        if msg.error():
            print(f"Error msg: {msg.error()}")
            continue
    
        #notification_service - send before some action was done   
        if msg.topic() == NOTIFICATION_SERVICE:
            event_in = KE_Notification_service_IN()    
            event_in.from_msg(msg)        
            print(f"Notification Center #633: {NOTIFICATION_SERVICE} action:[{event_in.corr_id}] [{event_in.action_id}]: {event_in}")

            # #list of services which can send sms/email #TODO: move that later to separated technical table
            message_services_details = {'sms_service':'details_sms_service_contact',
                                        'email_service':'details_email_service_contact'}
            

            with get_db() as session_wt_db:
                #get all data from tank_feature 
                wtf =RepositoryWaterTankFeatures(session=session_wt_db)                
                data_for_tank = wtf.select_all_for_tank(event_in.tank_tag)

            #if None - tank do not exist, end that iteration
            if data_for_tank is None:
                print(f"Notification Center #620:[{event_in.corr_id}] [{event_in.action_id}]: tank tag does not exist") 
                continue

            #check what msg services are online    
            dict_with_services_online = {}            
            for k,v in message_services_details.items():
                if getattr(data_for_tank,k) == 1:
                    dict_with_services_online[k]=getattr(data_for_tank,v) 

            
            if len(dict_with_services_online) == 0:
                print(f"Notification Center #634:[{event_in.corr_id}] [{event_in.action_id}]: all msg services are OFFLINE") 
                continue

            else:
                print(f"Notification Center #635:[{event_in.corr_id}] [{event_in.action_id}]: msg services: {[dict_with_services_online.keys()]} are ONLINE")                  
                print(f"Notification Center #636:[{event_in.corr_id}] [{event_in.action_id}]: msg services: {[dict_with_services_online.values()]} are ONLINE")                  

                #add info about services to incoming data, and send it to redis
                run.send_to_redis(unique_key=f'{event_in.corr_id}{event_in.action_id}',
                                basic_service_data=event_in.get_data(),
                                additional_data={'additional_data':dict_with_services_online})            


        elif msg.topic() == FRONT_API_RETURN_MSG:   
            event_in = KE_Front_api_return_msg_IN()  
            event_in.from_msg(msg)   

            #collect data from mgs   
            redis_data_py_model = run.get_from_redis(f"{event_in.corr_id}{event_in.action_id}")
            
            if redis_data_py_model is None:  
                print(f"Notification Center #701:[{event_in.corr_id}] [{event_in.action_id}]: msg services are OFFLINE")
                continue
            else:                
                print(f"Notification Center #702:[{event_in.corr_id}] [{event_in.action_id}]: redis_data_py_model: {redis_data_py_model}")

                run.send_messages_to_external_services(redis_data_py_model,msg.headers())    
        
    # except Exception as e:
    #     print(f"ERROR: {service_name}: {e}")
    # except KeyboardInterrupt:
    #     print(f"Log: {service_name} service is stoping")
    # finally:
    #     run.get_kafka_consumer().close()

if __name__ == "__main__":
    main()
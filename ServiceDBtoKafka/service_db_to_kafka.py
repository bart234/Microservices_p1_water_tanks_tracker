import time,os,json,uuid
from functools import partial
from sqlalchemy.orm import sessionmaker,Session
from sqlalchemy import create_engine
from dotenv import load_dotenv
from confluent_kafka import Producer
from db_access_layer_notification import RepositoryNotification_tab,RepositoryMaintenenceData
from storage_for_function import delivery_report
from get_secrets import get_secrets

load_dotenv()

prod_mode =bool(os.getenv("PROD_MODE", "False").lower())
t = int(os.getenv("WAIT_TIME", "1"))
limit = int(os.getenv("RECORD_LIMIT", "1"))
producer = Producer({'bootstrap.servers': 'kafka:29092'})
callback_desc = "DB_pickup->Kafka:"

service = 'db_to_kafka'                   #topic
service_name = 'db to kafka service'    #desc
callback_desc = "DB -> KAFKA:"
notification_callback = "DB -> NOTIFICATION_CENTER:"
KAFKA_BOOTSTRAP_SERVER_CONSUMER ="kafka:29092"

#INPUT:
WAIT_TIME = 5       #time between pools
KAFKA_TOPIC_CONSUMER = [service]
KAFKA_CONSUMER_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVER_CONSUMER,
    "group.id": f'{service}.WaterTank',
    "auto.offset.reset": "earliest"}


#OUTPUT:
# KAFKA_RETURN_TOPIC_FALLBACK_NOTIF = 'front_api_return_msg'
KAFKA_PRODUCER_CONFIG = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVER_CONSUMER}


DATABASE_URL="postgresql://{user}:{pwd}@{db_host}:5432/{db_name}".format(user=get_secrets(os.getenv("POSTGRES_USER_FILE", "user")),
                                                                          pwd=get_secrets(os.getenv("POSTGRES_PASSWORD_FILE", "pwd")),
                                                                          db_host=get_secrets(os.getenv("POSTGRES_DB_LOCALHOST", "pwd")),
                                                                          db_name=get_secrets(os.getenv("POSTGRES_DB_FILE", "pwd")))
engine = create_engine(DATABASE_URL,echo=False)
Session=sessionmaker(bind=engine)    

#TODO: think do keep that list in db, to separate resposibility
topic_list_for_notification_center = ['water_tank_power',"sms_service","email_service"] #topic list which may trigger  notification


#.env file
# user = os.getenv("POSTGRES_USER_FILE", "postgres")
# pwd = os.getenv("POSTGRES_PASSWORD_FILE")
# db_host = os.getenv("POSTGRES_DB_LOCALHOST", "127.0.0.1")
# db_name = os.getenv("POSTGRES_DB_FILE", "postgres")

#OUTPUT:
#header: corr_id(from db): ... action_id(generated here):.... 


def main():
    try:
        while True:
            time.sleep(t)
            session=Session()
            try:
                db = RepositoryNotification_tab(session)

                print(".")

                #---------------------------------------DB part-------------------------------------------------
                #get notification to send father
                result = db.select_last_n_notprocessed(limit=limit)

                #move db obj to list to futher work with that
                #DEPENDENCY on data_for_kafka - result have to be sorted
                data_for_kafka = {f"{item.tank_tag}-{item.kafka_msg_group}-{item.field_changed}":
                                {'kafka_topic':item.kafka_msg_group,
                                'tank_tag':item.tank_tag,
                                'field_to_update':item.field_changed,
                                'new_value':item.new_value} for item in result}

                
                #update records in Notification table- message was recived
                db.update_not_processed_to_processed(1,[item.id for item in result])
            
                #get corr_id from table and add into , out: {'22344': 'e0de929cea7446a49b5d35b6bb95c84a'}
                maintenence_db = RepositoryMaintenenceData(session)
                tank_tag_and_corr_id = maintenence_db.get_corr_id_from_list([v['tank_tag'] for v in data_for_kafka.values()])
                for k,v in data_for_kafka.items():
                    v['corr_id']=tank_tag_and_corr_id[v['tank_tag']]

                #first db commit
                if prod_mode:
                    session.commit()
                #-------------------------------------end DB part ----------------------------------------------------
                                 
                for _,v in data_for_kafka.items(): 
                    action_id= uuid.uuid4().hex    
                    corr_id = v['corr_id']      
                    print(f"Log [{corr_id}][{action_id}][{v['tank_tag']}]: {callback_desc} (data) {v}",flush=True)
                    headers_list =[("corr_id",(corr_id).encode("utf-8")),
                                   ("action_id",(action_id).encode("utf-8"))]
                    bound_callback = partial(delivery_report,callback_desc=callback_desc,corr_id=corr_id,action_id=action_id,tank_tag=v['tank_tag'])
                    producer.produce(topic=v['kafka_topic'],
                                    value=json.dumps(v).encode("utf-8"),
                                    headers=headers_list,
                                    callback=bound_callback)
               
                    
                    if v['kafka_topic'] in topic_list_for_notification_center:
                        producer.produce(topic='notification_service',
                                        value=json.dumps(v).encode("utf-8"),
                                        headers=headers_list
                                        )
                        print(f"Log [{v['corr_id']}][{action_id}][{v['tank_tag']}]: {notification_callback} Send",flush=True)
                
                #second kafka commit
                producer.flush()

                #clear kafka data                
                data_for_kafka={}

            except Exception as e:
                session.rollback()
                data_for_kafka={}
                
                print(f"ERROR: During Notification tab update: {e}")
            finally:
                session.close()

    except KeyboardInterrupt as e:
        print("Log: App done")


if __name__ == "__main__":
    main()
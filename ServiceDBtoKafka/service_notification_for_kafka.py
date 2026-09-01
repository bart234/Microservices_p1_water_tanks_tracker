import time,os,json
from sqlalchemy.orm import sessionmaker,Session
from sqlalchemy import create_engine
from dotenv import load_dotenv
from confluent_kafka import Producer

from db_access_layer_notification import RepositoryNotification_tab,RepositoryMaintenenceData
from get_secrets import get_secrets

load_dotenv()

prod_mode =bool(os.getenv("PROD_MODE", "False").lower())
t = int(os.getenv("WAIT_TIME", "1"))
limit = int(os.getenv("RECORD_LIMIT", "1"))
producer = Producer({'bootstrap.servers': 'kafka:29092'})
callback_desc = "DB_pickup->Kafka:"


DATABASE_URL="postgresql://{user}:{pwd}@{db_host}:5432/{db_name}".format(user=get_secrets(os.getenv("POSTGRES_USER_FILE", "user")),
                                                                          pwd=get_secrets(os.getenv("POSTGRES_PASSWORD_FILE", "pwd")),
                                                                          db_host=get_secrets(os.getenv("POSTGRES_DB_LOCALHOST", "pwd")),
                                                                          db_name=get_secrets(os.getenv("POSTGRES_DB_FILE", "pwd")))
engine = create_engine(DATABASE_URL,echo=False)
Session=sessionmaker(bind=engine)    

#.env file
# user = os.getenv("POSTGRES_USER_FILE", "postgres")
# pwd = os.getenv("POSTGRES_PASSWORD_FILE")
# db_host = os.getenv("POSTGRES_DB_LOCALHOST", "127.0.0.1")
# db_name = os.getenv("POSTGRES_DB_FILE", "postgres")



#here in msg we have corr_id, in other cases we have to pass it as arg to partial f
def delivery_report(err,msg,callback_desc=callback_desc):
    if err: 
        print(f"Log: Delivery error {err}")
    else:        
        v=json.loads(msg.value().decode("utf-8"))
        print(f"Log [{v['corr_id']}][{v['tank_tag']}]: {callback_desc} Delivered",flush=True)

def main():
    try:
        while True:
            time.sleep(t)
            session=Session()
            try:
                db = RepositoryNotification_tab(session)

                print("Log: Im waiting for data")
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
                list_to_update = [item.id for item in result]
                number_of_updated_rows = db.update_not_processed_to_processed(1,list_to_update)
            
                #get corr_id from table and add into , out: {'22344': 'e0de929cea7446a49b5d35b6bb95c84a'}
                maintenence_db = RepositoryMaintenenceData(session)
                tank_tag_and_corr_id = maintenence_db.get_corr_id_from_list([v['tank_tag'] for v in data_for_kafka.values()])
                for k,v in data_for_kafka.items():
                    v['corr_id']=tank_tag_and_corr_id[v['tank_tag']]


                for a,v in data_for_kafka.items():     
                            print(f"Log [{v['corr_id']}][{v['tank_tag']}]: DB_pickup->Kafka: {v}",flush=True)
                            producer.produce(topic=v['kafka_topic'],
                                            value=json.dumps(v).encode("utf-8"),
                                            headers=[("corr_id",(v['corr_id']).encode("utf-8"))],
                                            callback=delivery_report)
                
                

                #first db commit
                if prod_mode:
                    session.commit()

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
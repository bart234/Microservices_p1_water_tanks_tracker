import time,os
from model_notification import Notification_tab
from db_access_layer_notification import RepositoryNotification_tab
from sqlalchemy.orm import sessionmaker,Session
from sqlalchemy import create_engine
from dotenv import load_dotenv
from confluent_kafka import Producer
import uuid,json

load_dotenv()

def get_secrets(secret_name):
    try:
        with open(f"{secret_name}",'r') as file:
            return file.read().strip()
    except IOError:
        return os.getenv(secret_name)


DATABASE_URL="postgresql://{user}:{pwd}@{db_host}:5432/{db_name}".format(user=get_secrets(os.getenv("POSTGRES_USER_FILE", "user")),
                                                                          pwd=get_secrets(os.getenv("POSTGRES_PASSWORD_FILE", "pwd")),
                                                                          db_host=get_secrets(os.getenv("POSTGRES_DB_LOCALHOST", "pwd")),
                                                                          db_name=get_secrets(os.getenv("POSTGRES_DB_FILE", "pwd")))

producer = Producer({'bootstrap.servers': 'kafka:29092'})
def delivery_report(err,msg):
    if err: 
        print(f"Log: Delivery error {err}")
    else:
        print(f"Log: NotificationService->Kafka: delivered: {msg.value().decode("utf-8")}")

#.env file
# user = os.getenv("POSTGRES_USER_FILE", "postgres")
# pwd = os.getenv("POSTGRES_PASSWORD_FILE")
# db_host = os.getenv("POSTGRES_DB_LOCALHOST", "127.0.0.1")
# db_name = os.getenv("POSTGRES_DB_FILE", "postgres")

prod_mode =bool(os.getenv("PROD_MODE", "False").lower())
t = int(os.getenv("WAIT_TIME", "1"))
limit = int(os.getenv("RECORD_LIMIT", "1"))



engine = create_engine(DATABASE_URL,echo=False)

Session=sessionmaker(bind=engine)    
session=Session()
try:
    while True:
        time.sleep(t)
        try:
            db = RepositoryNotification_tab(session)

            print("Log: Im waiting for data")
            #get notification to send father
            result = db.select_last_n_notprocessed(limit=limit)

            #move db obj to list to futher work with that
            #DEPENDENCY on data_for_kafka - result have to be sorted
            data_for_kafka = {f"{item.tank_tag}-{item.kafka_msg_group}-{item.field_changed}":
                              {'kafka_topic':item.kafka_msg_group,'tank_tag':item.tank_tag,'field_to_update':item.field_changed,'new_value':item.new_value} for item in result}
            
            #extract id and upgrasde that records as done
            list_to_update = {item.id for item in result}
            number_of_updated_rows = db.update_not_processed_to_processed(1,list_to_update)
            if prod_mode:
                session.commit()

        except Exception as e:
            session.rollback()
            data_for_kafka={}
            
            print("ERROR: During Notification tab update")
        finally:
            session.close()

        for a,v in data_for_kafka.items():     
            print(f"Log: NotificationService->Kafka: {v}",flush=True)
            producer.produce(topic=v['kafka_topic'],
                             value=json.dumps(v).encode("utf-8"),
                             callback=delivery_report)

        #clear kafka data
        data_for_kafka={}
        producer.flush()

except KeyboardInterrupt as e:
    print("Log: App done")
  
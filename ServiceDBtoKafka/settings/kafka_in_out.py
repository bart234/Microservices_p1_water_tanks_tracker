import os

#---------------- variable from env -----------------
KAFKA_SERVER_ADDRESS =          os.getenv("KAFKA_SERVER_ADDRESS","kafka:29092")
KAFKA_GROUP_ID =                os.getenv("KAFKA_GROUP_ID","DbtoKafka.id")
KAFKA_WAIT_TIME_BETWEE_POOLS =  float(os.getenv("KAFKA_WAIT_TIME_BETWEE_POOLS",5)) 

# producer (only producer in that service)
KAFKA_TOPIC_OUTGOING =          os.getenv("KAFKA_TOPIC_OUTGOING","from tab notification.kafka_msg_group")
KAFKA_TOPIC_OUTGOING_NOTIF_CENTER =  os.getenv("KAFKA_TOPIC_OUTGOING_NOTIF_CENTER","notification_service")

#for Azure
KAFKA_EVENT_HUB_CONNECTION_STRING=  os.getenv("KAFKA_EVENT_HUB_CONNECTION_STRING")
KAFKA_EVENT_HUB_NAMESPACE=          os.getenv("KAFKA_EVENT_HUB_NAMESPACE")


#--------------------- just logic -------------------
if KAFKA_EVENT_HUB_CONNECTION_STRING:
    kafka_out_cfg = {'KAFKA_TOPIC_PRODUCER':KAFKA_TOPIC_OUTGOING,       
                    'KAFKA_PRODUCER_CONFIG' : {
                            'bootstrap.servers':f"{KAFKA_EVENT_HUB_NAMESPACE}.servicebus.windows.net:9093",
                            "security.protocol": "SASL_SSL",
                            "sasl.mechanism": "PLAIN",
                            "sasl.username": "$ConnectionString",
                            "sasl.password": KAFKA_EVENT_HUB_CONNECTION_STRING,
                            "acks": "all",
                            "retries": 10,
                            "retry.backoff.ms": 500,
                            "linger.ms": 5,
                            "batch.num.messages": 1000,
                            "request.timeout.ms": 30000,
                            "message.timeout.ms": 60000}
                    }
else:
    kafka_out_cfg = {'KAFKA_TOPIC_PRODUCER':KAFKA_TOPIC_OUTGOING,       
                    'KAFKA_PRODUCER_CONFIG' :
                            {'bootstrap.servers':KAFKA_SERVER_ADDRESS}
                    }
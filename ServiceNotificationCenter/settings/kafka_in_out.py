import os
KAFKA_WAIT_TIME_BETWEE_POOLS =  int(os.getenv("KAFKA_WAIT_TIME_BETWEE_POOLS",5)) 
KAFKA_SERVER_ADDRESS =          os.getenv("KAFKA_SERVER_ADDRESS","kafka:29092")
raw_KAFKA_INCOMING_TOPICS =     os.getenv("KAFKA_INCOMING_TOPICS",)
KAFKA_GROUP_ID =                os.getenv("KAFKA_GROUP_ID","NotificationCenter.id")
KAFKA_TOPIC_TEMPLATE=           os.getenv("KAFKA_TOPIC_TEMPLATE","topic.message")

KAFKA_INCOMING_TOPICS = [topic.strip() for topic in raw_KAFKA_INCOMING_TOPICS.split(",")] \
                                if raw_KAFKA_INCOMING_TOPICS else ['notification_service','front_api_return_msg']

kafka_in_cfg = { 'WAIT_TIME': 5,      #time between pools
                 'KAFKA_TOPIC_CONSUMER' : KAFKA_INCOMING_TOPICS,
                 'KAFKA_CONSUMER_CONFIG' : {
                                            "bootstrap.servers": KAFKA_SERVER_ADDRESS,
                                            "group.id": KAFKA_GROUP_ID,
                                            "auto.offset.reset": "earliest"}
                }

#topic.message - 'topic' will be replaced by expected service like: sms_service.message or email_service.message
kafka_out_cfg = {'KAFKA_RETURN_TOPIC_FALLBACK_NOTIF':KAFKA_TOPIC_TEMPLATE,       
                 'KAFKA_PRODUCER_CONFIG' :
                         {'bootstrap.servers':KAFKA_SERVER_ADDRESS}
                }

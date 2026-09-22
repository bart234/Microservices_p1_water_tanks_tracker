import os
#consumer
KAFKA_WAIT_TIME_BETWEE_POOLS =  float(os.getenv("KAFKA_WAIT_TIME_BETWEE_POOLS",5)) 
KAFKA_SERVER_ADDRESS =          os.getenv("KAFKA_SERVER_ADDRESS","kafka:29092")
raw_KAFKA_INCOMING_TOPICS =     os.getenv("KAFKA_INCOMING_TOPICS",)
KAFKA_GROUP_ID =                os.getenv("KAFKA_GROUP_ID","ServiceSMS.id")

#producer
KAFKA_TOPIC_OUTGOING =          os.getenv("KAFKA_TOPIC_OUTGOING","front_api_message_service")

if "," in raw_KAFKA_INCOMING_TOPICS:
    KAFKA_INCOMING_TOPICS = [topic.strip() for topic in raw_KAFKA_INCOMING_TOPICS.split(",")] \
                                    if raw_KAFKA_INCOMING_TOPICS else ['notification_service','front_api_return_msg']
else:
    KAFKA_INCOMING_TOPICS = [raw_KAFKA_INCOMING_TOPICS]

kafka_in_cfg = { 'WAIT_TIME': KAFKA_WAIT_TIME_BETWEE_POOLS,      #time between pools
                 'KAFKA_TOPIC_CONSUMER' : KAFKA_INCOMING_TOPICS,
                 'KAFKA_CONSUMER_CONFIG' : {
                                            "bootstrap.servers": KAFKA_SERVER_ADDRESS,
                                            "group.id": KAFKA_GROUP_ID,
                                            "auto.offset.reset": "earliest"}
                }

#topic.message - 'topic' will be replaced by expected service like: sms_service.message or email_service.message
kafka_out_cfg = {'KAFKA_TOPIC_PRODUCER':KAFKA_TOPIC_OUTGOING,       
                 'KAFKA_PRODUCER_CONFIG' :
                         {'bootstrap.servers':KAFKA_SERVER_ADDRESS}
                }

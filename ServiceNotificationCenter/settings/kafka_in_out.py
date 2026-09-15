
KAFKA_SERVER_ADDRESS = "kafka:29092"
topics_to_catch = ['notification_service','front_api_return_msg']
group_id = 'NotificationCenter.id'

kafka_in_cfg = { 'WAIT_TIME': 5,      #time between pools
                 'KAFKA_TOPIC_CONSUMER' : topics_to_catch,
                 'KAFKA_CONSUMER_CONFIG' : {
                                            "bootstrap.servers": KAFKA_SERVER_ADDRESS,
                                            "group.id": group_id,
                                            "auto.offset.reset": "earliest"}
                }

#topic.message - 'topic' will be replaced by expected service like: sms_service.message or email_service.message
kafka_out_cfg = {'KAFKA_RETURN_TOPIC_FALLBACK_NOTIF':'topic.message',       
                 'KAFKA_PRODUCER_CONFIG' :
                         {'bootstrap.servers':KAFKA_SERVER_ADDRESS}
                }

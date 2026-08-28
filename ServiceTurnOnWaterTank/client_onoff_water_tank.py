from confluent_kafka import Consumer
import json

consumer_config = {"bootstrap.servers":"localhost:9092",
                   "group.id":"sms_notification_group",
                   "auto.offset.reset":"earliest"}

consumer_ = Consumer(consumer_config)

consumer_.subscribe(['sms_notification'])

print("SMS NOtificaion service is running")

try:
    while True:
        msg = consumer_.poll(5.0)   #
        if msg is None:
            continue
        if msg.error():
            print(f"Error: {msg.error()}")

        msg_uncoded = msg.value().decode("utf-8")
        data_from_msg = json.load(msg_uncoded)
        #expected msg: {"tank_tag":... , "phone_num":....}
        if msg.topic() == "sms_notification":
            print(f"Service on: tank_tag: {data_from_msg["tank_tag"]}")


except KeyboardInterrupt:
    print("Service: Sms Notification is going to close")



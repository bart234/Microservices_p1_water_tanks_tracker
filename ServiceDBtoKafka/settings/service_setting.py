import os

# ---------------- service functionality variable --------------------
#PROD_MODE - if it is True, changes wich are were requested (like: turn ON water tank) are not commited to db
#            but whole messageing ssytem is running normally, so msg to output_service and notification_center 
#            will be created
PROD_MODE =     os.getenv("PROD_MODE", "False").lower()

#RECORD_LIMIT - that variable response for how many not processed records will be load at once 
#               it doesnt matter what event and what tank , it is databade restriction 
#               it work with SLEEP_TIME, whole mechanism  request n-number of records on each x -time in loop
#               allow to limit  db request and traffic by service
RECORD_LIMIT =  os.getenv("RECORD_LIMIT", "10")
SLEEP_TIME =    os.getenv("SLEEP_TIME", "10")

#TOPIC_LIST_FOR_NOTIFICATION_CENTER - if base_event service (like water_tank_power) is on the list then if that event will occure
#               system will send additional message to notification_center with topic: notification_center
#               notification_center will store the msg for n-time and will match it with msg from base_event return msg (that back msg went to api) 
#               if they match notification process will start (to send sms/email/etc)
raw_TOPIC_LIST_FOR_NOTIFICATION_CENTER = os.getenv("TOPIC_LIST_FOR_NOTIFICATION_CENTER", "1")


# ---------------------------- just logic-----------------------------
if "," in raw_TOPIC_LIST_FOR_NOTIFICATION_CENTER:
    TOPIC_LIST_FOR_NOTIFICATION_CENTER = [topic.strip() for topic in raw_TOPIC_LIST_FOR_NOTIFICATION_CENTER.split(",")] \
                                    if raw_TOPIC_LIST_FOR_NOTIFICATION_CENTER else ['water_tank_power',"sms_service","email_service"]
else:
    TOPIC_LIST_FOR_NOTIFICATION_CENTER = [raw_TOPIC_LIST_FOR_NOTIFICATION_CENTER]

# ------------------- service important setting-----------------------
service_cfg = {
    'prod_mode':    bool(PROD_MODE),
    'record_limit': int(RECORD_LIMIT),
    'sleep_time':   int(SLEEP_TIME),
    'topic_list_for_notification_center': TOPIC_LIST_FOR_NOTIFICATION_CENTER,
}

# ---------------- service messaging and loging setting---------------

service_setting = {
                    "service_name":     'Db to kafka',             
                    "log_description":  "Water Tank PowerService-> Sent:",
                    "service_callback":    "DB -> KAFKA(Delivered) -> SERVICE",
                    "notification_callback": "DB -> KAFKA(Delivered)-> NOTIFICATION_CENTER",
                    }   
service_setting.update(service_cfg)
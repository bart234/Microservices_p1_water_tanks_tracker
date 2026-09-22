#TEMPLATE
from ServiceSMS.main_logic.main_service_run_cls import MainServiceRun
from ServiceSMS.models.kafka_data_models.kafka_incoming_data import KE_SmsService_IN
from ServiceSMS.models.kafka_data_models.kafka_outgoing_data import KE_Front_Api_Message_Service_OUT
from ServiceSMS.parer_logic.sms_logic_creation import PrepareSMS

from ServiceSMS.settings.kafka_in_out import kafka_in_cfg,kafka_out_cfg
from ServiceSMS.settings.service_setting import service_setting
from .storage_for_function import delivery_report
import datetime

def main():     
    run = MainServiceRun()

    #load consumer and producer config from dict
    run.load_settings_from_cfgs(None,kafka_in_cfg,kafka_out_cfg,service_setting)

    #set consumer / consumer
    run._set_kafka_consumer_cfg()
    run._set_kafka_producer_cfg()

    #get consumer
    consumer_=run.get_kafka_consumer()

    print(f"Log: {service_setting["service_name"]} service is running")   
    try:
        while True:
            msg=consumer_.poll(run.get_KAFKA_WAIT_TIME())
            if msg is None:
                continue
            if msg.error():
                print(f"Error msg: {msg.error()}")
                continue

            #gather data from kafka, validate 
            incoming_event = KE_SmsService_IN()
            incoming_event.from_msg(msg)

            #main logic: prepare and send sms
            sms_object = PrepareSMS(incoming_event,service_setting["service_name"])
            sms = sms_object.prepare_sms()
            log_core = f"[{incoming_event.corr_id}][{incoming_event.action_id}][{incoming_event.tank_tag}]"
            if sms_object.send_sms(sms):
                print(f"Log {log_core}: {service_setting["service_name"]}: SMS Sent")
            else:
                print(f"Log {log_core} {service_setting["service_name"]}: SMS Error")
                continue

            #prepare output KE and validate data
            output_event=KE_Front_Api_Message_Service_OUT()
            switch_dict_smsIN_to_FrontApiOUT = {"header":{'corr_id':incoming_event.corr_id,
                                                          'action_id':incoming_event.action_id},
                                                "data":{'service_time':(datetime.datetime.now(datetime.timezone.utc)).isoformat(),
                                                        'tank_tag':incoming_event.tank_tag,
                                                        'action':str(sms),
                                                        'base_action': "SMS Sent"}}            
            output_event.from_dict(switch_dict_smsIN_to_FrontApiOUT)

            #prepare Log variable for producer callback_function
            log_details = {'corr_id':incoming_event.corr_id,
                            'action_id':incoming_event.action_id,
                            'tank_tag':incoming_event.tank_tag,
                            'log_description': run.service_setting['log_description']}

            #producer
            run.send_messages_to_external_services(outcoming_event=output_event,
                                                   kafka_event=run.kafka_out_cfg['KAFKA_TOPIC_PRODUCER'],
                                                   callback_function_and_args=(delivery_report,log_details))            

     

    except KeyboardInterrupt:
        print(f"Log: {service_setting["service_name"]} service is stoping")
    finally:
        consumer_.close()

if __name__ == "__main__":
    main()
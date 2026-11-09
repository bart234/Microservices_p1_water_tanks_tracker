# #TEMPLATE
from ServiceDBtoKafka.main_logic.main_service_run_cls import MainServiceRun
from ServiceDBtoKafka.main_logic.service_custom_logic import Service_DBtoKafka
from ServiceDBtoKafka.settings.kafka_in_out import kafka_out_cfg
from ServiceDBtoKafka.settings.db_setting import DATABASE_URL
from ServiceDBtoKafka.settings.service_setting import service_setting
from ServiceDBtoKafka.models.kafka_data_models.kafka_out_data import KE_CustomEvent_OUT, KE_Notification_service_OUT



def main(service_logic,models_list,kafka_in_cfg,kafka_out_cfg,service_setting):    
    
    run = MainServiceRun(service_logic,
                        models_list,
                        kafka_out_cfg=kafka_out_cfg,
                        service_setting=service_setting,
                        db_conn_string=DATABASE_URL)

    run.service_main_loop()

if __name__ == "__main__":
    main(Service_DBtoKafka,
         [KE_CustomEvent_OUT, KE_Notification_service_OUT],
         None,
         kafka_out_cfg,
         service_setting)
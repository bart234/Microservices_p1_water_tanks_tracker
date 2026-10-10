# #TEMPLATE
from ServiceTurnOnWaterTank.main_logic.main_service_run_cls import MainServiceRun
from ServiceTurnOnWaterTank.main_logic.service_custom_logic import Service_WaterTankPower
from ServiceTurnOnWaterTank.settings.kafka_in_out import kafka_in_cfg,kafka_out_cfg
from ServiceTurnOnWaterTank.settings.service_setting import service_setting
from ServiceTurnOnWaterTank.models.kafka_data_models.kafka_incoming_data import KE_WaterTankPower_IN
from ServiceTurnOnWaterTank.models.kafka_data_models.kafka_outgoing_data import KE_Front_Api_Message_Service_OUT



def main(service_logic,models_list,kafka_in_cfg,kafka_out_cfg,service_setting):    
    
    run = MainServiceRun(service_logic,
                        models_list,
                        kafka_in_cfg=kafka_in_cfg,
                        kafka_out_cfg=kafka_out_cfg,
                        service_setting=service_setting)

    run.service_main_loop()

if __name__ == "__main__":
    main(Service_WaterTankPower,
         [KE_WaterTankPower_IN,KE_Front_Api_Message_Service_OUT],
         kafka_in_cfg,
         kafka_out_cfg,
         service_setting)
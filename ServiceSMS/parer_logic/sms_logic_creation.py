class PrepareSMS:
    def __init__(self,KE_SmsService_IN,service_name):
        self.service_name = service_name
        self.KE_event = KE_SmsService_IN

    def prepare_sms(self)-> str:
        try:
            if self.KE_event.base_action == "water_tank_power":
                if self.KE_event.field_to_update == "turnOnOff":                    
                    return_msg = f'SMS Text (Tank {self.KE_event.tank_tag} is switch {'on' if self.KE_event.new_value =='1' else 'off'})'
                elif self.KE_event.field_to_update == "message":
                   return_msg = f'SMS Text (Tank {self.KE_event.tank_tag}: message: {self.KE_event.new_value})'
                else:
                    return_msg = f'SMS Text (Tank {self.KE_event.tank_tag}: action: None)'
            else:
                return_msg = f'Undefined action on SMS Service:  Tank {self.KE_event.tank_tag}'
            return return_msg
        except Exception as e:
            print(f"ERROR-Log [{self.KE_event.corr_id}][{self.KE_event.action_id}][{self.KE_event.tank_tag}]: {self.service_name}: {e}")

    def send_sms(self,sms)->bool:
        print(f"SMS TO USER: {sms}")                           
        return True
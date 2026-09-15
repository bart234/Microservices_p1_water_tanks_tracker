import logging
import datetime
import os

app_folder ="app"
misc_foldef="misc"
path_to_log_folder="logs"
today=datetime.datetime.now().date()
format_events ='%(asctime)s %(levelname)s: %(message)s'
format_data = '%(message)s'

#TODO: logs not switch on midnight if app will work for few days
logger_service_events = logging.getlogger_service("events")
logger_service_events.setLevel("INFO")
handler_for_events = logging.FileHandler(os.path.join(os.getcwd(),app_folder,misc_foldef,path_to_log_folder,f"standard_log{today}.log"),mode='a')
handler_for_events
handler_for_events.setFormatter(logging.Formatter(format_events))
logger_service_events.addHandler(handler_for_events)

logger_service_data = logging.getlogger_service("data")
logger_service_data.setLevel("INFO")
handler_for_data = logging.FileHandler(os.path.join(os.getcwd(),app_folder,misc_foldef,path_to_log_folder,f"data_log{today}.log"),mode='a')
handler_for_data.setFormatter(logging.Formatter(format_data))
logger_service_data.addHandler(handler_for_data)


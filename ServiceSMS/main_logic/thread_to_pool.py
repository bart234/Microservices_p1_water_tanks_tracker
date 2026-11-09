import threading
import time

def start_producer_background_loop(producer):
    def run_loop():
        while True:
            producer.poll(0.1)
            time.sleep(0.1) 

    t = threading.Thread(target=run_loop, daemon=True) #daemon will close th after app close
    t.start()
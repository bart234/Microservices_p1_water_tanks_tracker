import os


redis_cfg = {   'REDIS_TIME_TO_EXPIRE_KEY' :    os.getenv("REDIS_TIME_TO_EXPIRE_KEY",20),
                'REDIS_HOST' :                  os.getenv("REDIS_HOST","inf_redis"),      
                'REDIS_PORT' :                  os.getenv("REDIS_PORT","6379")
            }   
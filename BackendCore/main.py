from fastapi import FastAPI
from BackendCore.db_cfg import engine,Base
from BackendCore.api.routers import tank_path
from BackendCore.settings.kafka_in_out import kafka_in_cfg, kafka_out_cfg,KAFKA_EVENT_HUB_CONNECTION_STRING
from contextlib import asynccontextmanager
import asyncio,json
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from aiokafka import AIOKafkaConsumer

kafka_task = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global kafka_task
    Base.metadata.create_all(bind=engine)
    #async function will run function consume_kafka_callbacks as backgound
    #consume_kafka_callbacks - will listen in background on desired topics
    kafka_task =asyncio.create_task(consume_kafka_callbacks())
    yield
    if kafka_task:
        kafka_task.cancel()
        try:
            await kafka_task
        except asyncio.CancelledError:
            print("Kafka client is going to close")


app = FastAPI(lifespan=lifespan)
app.include_router(tank_path.router)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # change if not in development
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
active_connections: list[WebSocket] = []

def get_from_headers(msg,element_to_get)-> str:
    try:
        header_dict ={k: v.decode('utf-8') for k,v in msg.headers()}
        return header_dict.get(element_to_get)
    except:
        return None
                
@app.get("/")
def main():
    return {"msg":"bob"}


#that endpoint expected to have websocket conenction,
#if browser does - it will add that conenciton to list
#where kafka's mssage will be distributed 
@app.websocket("/ws/logs")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    active_connections.append(websocket)
    try:
        while True:
            await websocket.receive_text() # Utrzymuje połączenie, ignoruje przychodzące wiadomości z frontu
    except WebSocketDisconnect:
        active_connections.remove(websocket)

async def consume_kafka_callbacks():
    if KAFKA_EVENT_HUB_CONNECTION_STRING:        
        import ssl
        ssl_context = ssl.create_default_context()
        
        consumer = AIOKafkaConsumer(*kafka_in_cfg['KAFKA_TOPIC_CONSUMER'],
                                    bootstrap_servers=kafka_in_cfg['KAFKA_CONSUMER_CONFIG']["bootstrap.servers"],
                                    security_protocol=kafka_in_cfg['KAFKA_CONSUMER_CONFIG']["security.protocol"],
                                    sasl_mechanism=kafka_in_cfg['KAFKA_CONSUMER_CONFIG']["sasl.mechanism"],
                                    sasl_plain_username=kafka_in_cfg['KAFKA_CONSUMER_CONFIG']["sasl.username"],
                                    sasl_plain_password=kafka_in_cfg['KAFKA_CONSUMER_CONFIG']["sasl.password"],
                                    group_id=kafka_in_cfg['KAFKA_CONSUMER_CONFIG']["group.id"],
                                    auto_offset_reset=kafka_in_cfg['KAFKA_CONSUMER_CONFIG']["auto.offset.reset"],
                                    ssl_context=ssl_context)   

        await consumer.start()
    else:
        consumer = AIOKafkaConsumer(*kafka_in_cfg['KAFKA_TOPIC_CONSUMER'],
                                bootstrap_servers=kafka_in_cfg['KAFKA_CONSUMER_CONFIG']["bootstrap.servers"],
                                group_id=kafka_in_cfg['KAFKA_CONSUMER_CONFIG']["group.id"],
                                auto_offset_reset=kafka_in_cfg['KAFKA_CONSUMER_CONFIG']["auto.offset.reset"])   
    print("Log: Kafka connection success.Consumer started")
    try:
        #it sit inbackground and if any msg appear from expected topic
        async for msg in consumer:
            in_data= msg.value.decode("utf-8")  
            data = json.loads(in_data)

            #msg.headers  (("item",b'324234234'),("item",b'324234234'))
            header_dict ={a:b.decode("utf-8") for a,b in msg.headers}
            
            print(f"Log [{header_dict['corr_id']}][{header_dict['action_id']}][{data['tank_tag']}]: Main to front:{msg.value.decode('utf-8')}",flush=True)

            # /ws/logs have list about conencted browsers
            #and loop will send every browser msg from kafka
            for connection in active_connections:
                try:
                    asyncio.create_task(connection.send_json(data))
                except Exception:
                    pass
    except Exception as e:
        print(f"Log: Kafka error during data gathering from services: {e}", flush=True)
    finally:
        await consumer.stop()
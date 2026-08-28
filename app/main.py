from fastapi import FastAPI
import app.models_data_structures as db_models
from app.db_cfg import engine,Base
from app.api.routers import tank_path
from contextlib import asynccontextmanager
import asyncio,json
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from aiokafka import AIOKafkaConsumer

Base.metadata.create_all(bind=engine)

@asynccontextmanager
async def lifespan(app: FastAPI):
    #async function will run function consume_kafka_callbacks as backgound
    #consume_kafka_callbacks - will listen in background on desired topics
    kafka_task =asyncio.create_task(consume_kafka_callbacks())
    yield
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

KAFKA_TOPIC_CONSUMER = ['service_return_msg']
KAFKA_BOOTSTRAP_SERVER_CONSUMER ="kafka:29092"
KAFKA_CONSUMER_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVER_CONSUMER,
    "group.id": KAFKA_TOPIC_CONSUMER,
    "auto.offset.reset": "earliest"
}


@app.get("/")
def main():
    return {"msg":"bob"}

#uvicorn main:app --relosdHello 

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
    consumer = AIOKafkaConsumer(
                                'service_return_msg',
                                bootstrap_servers="kafka:29092",
                                group_id='api_feedback_loop')      
     
    #'kafka:29092', - in docker network - it is called kafka
    #"fastapi_front_broadcaster"    

    await consumer.start()
    try:
        #it sit inbackground and if any msg appear from expected topic
        async for msg in consumer:
            print(msg.value.decode('utf-8'),flush=True)

            event_data = json.loads(msg.value.decode('utf-8'))
            
            # /ws/logs have list about conencted browsers
            #and loop will send every browser msg from kafka
            for connection in active_connections:
                try:
                    asyncio.create_task(connection.send_json(event_data))
                except Exception:
                    pass
    except Exception as e:
        print(f"Kafka error during data gathering from services: {e}", flush=True)
    finally:
        await consumer.stop()
import os

import uvicorn
from aiokafka import AIOKafkaConsumer
from fastapi import FastAPI
from fastapi import WebSocket
from fastapi.responses import FileResponse

app = FastAPI()


@app.get("/")
def get_dashboard():
    file_path = os.path.join(os.path.dirname(__file__), "madflow.html")
    return FileResponse(file_path)


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    topic = os.environ["KAFKA_TOPIC"]
    host = os.environ["KAFKA_HOST"]
    port = os.environ["KAFKA_PORT"]

    consumer = AIOKafkaConsumer(topic,
                                bootstrap_servers=f"{host}:{port}",
                                auto_offset_reset="earliest")
    try:
        await websocket.accept()
        while True:
            await consumer.start()
            async for msg in consumer:
                await websocket.send_text(msg.value.decode("utf-8"))

    except Exception:
        pass


if __name__ == '__main__':
    os.environ["KAFKA_TOPIC"] = "madflow-output-stream"
    os.environ["KAFKA_HOST"] = "localhost"
    os.environ["KAFKA_PORT"] = "9094"

    uvicorn.run("app:app", host="localhost", port=3000, reload=True)

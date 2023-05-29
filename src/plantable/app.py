from fastapi import FastAPI, Request

# from . import routers
import json

app = FastAPI(title="PlanTable")
# app.include_router(routers.s3.router)
# app.include_router(routers.kafka.router)


@app.get("/hello")
async def hello():
    return {"hello": "world!"}


@app.post("/webhook")
async def webhook(request: Request):
    body = await request.json()
    print(request.headers)
    print(body)
    return body

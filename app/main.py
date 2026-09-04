import json
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from kafka import KafkaProducer

app = FastAPI(title="Payment Producer Service")

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

class PaymentRequest(BaseModel):
    transaction_id: str
    user_id: str
    amount: float

@app.post("/process-payment")
async def create_payment(payment: PaymentRequest):
    event = payment.dict()
    event["status"] = "PENDING"
    
    # Produce event to Kafka
    producer.send("payment-events", value=event)
    producer.flush()
    
    return {"status": "SUCCESS", "message": "Transaction queued for processing", "data": event}
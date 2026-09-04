import json
import time
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'payment-events',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    group_id='payment-group'
)

# Simulated in-memory DB for idempotency tracking
processed_transactions = set()

print("Starting Payment Event Processing Consumer...")

for message in consumer:
    event = message.value
    tx_id = event["transaction_id"]

    # Idempotency Check
    if tx_id in processed_transactions:
        print(f"[SKIPPED] Duplicate transaction detected: {tx_id}")
        continue

    # Process & Persist Payment
    event["status"] = "COMPLETED"
    processed_transactions.add(tx_id)
    print(f"[PROCESSED] Payment ID: {tx_id} | Amount: ${event['amount']} | Status: {event['status']}")
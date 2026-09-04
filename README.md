# Event-Driven Payment Processing System

An asynchronous, event-driven payment processing platform engineered using Python, FastAPI, Apache Kafka, and PostgreSQL. The system decouples transaction ingestion from execution to ensure reliable, high-throughput event processing with strict idempotency controls.

---

## 🏗 System Architecture


[ Client Request ]
       │
       ▼
[ FastAPI API Gateway ] ──► (POST /process-payment)
       │
       ▼ (Produces JSON Event)
[ Apache Kafka Broker ] ──► (Topic: 'payment-events')
       │
       ▼ (Asynchronous Consumption)
[ Payment Consumer Worker ]
       │
       ├──► [ Idempotency Check ] (Filters duplicate transaction_ids)
       │
       └──► [ PostgreSQL DB ] ──► Persists COMPLETED Status


🛠 Tech Stack
1. Primary Language: Python 3.12
2. REST API Framework: FastAPI & Uvicorn
3. Event Streaming: Apache Kafka, Apache Zookeeper, kafka-python-ng
4. Database: PostgreSQL & SQLAlchemy / Psycopg2
5. Data Validation: Pydantic
6. Containerization: Docker, Docker Compose

🚀 Key Features
1. High-Throughput Ingestion: Lightweight FastAPI REST endpoint that pushes transaction requests directly to Kafka topics, returning fast asynchronous responses to clients.
2. Idempotent Consumer Logic: Deduplication checks preventing double-spending or duplicate transaction persistence during message redeliveries or network retries.
3. Decoupled Architecture: Full segregation of the ingestion layer (Producer) and transactional processing layer (Consumer) for independent horizontal scaling.
4. Containerized Infrastructure: Unified docker-compose.yml orchestration launching Zookeeper, Kafka, and PostgreSQL dependencies instantly.

📁 Repository Structure
event-driven-payment-system/
├── docker-compose.yml    # Orchestrates Kafka, Zookeeper, and PostgreSQL
├── requirements.txt      # Python dependencies
├── .gitignore            # Excludes venv, pycache, and environmental files
├── README.md             # System documentation
└── app/
    ├── __init__.py
    ├── main.py           # FastAPI server & Kafka Producer
    └── consumer.py       # Kafka Consumer with idempotency logic

💻 Local Setup & Execution Guide
Prerequisites
1. Docker Desktop installed and running
2. Python 3.10+


1. Clone the Repository & Start Containers
git clone [https://github.com/AnushaKhot/event-driven-payment-system.git](https://github.com/AnushaKhot/event-driven-payment-system.git)
cd event-driven-payment-system

# Start Zookeeper, Kafka, and PostgreSQL
docker-compose up -d

2. Virtual Environment Setup
   python -m venv venv

# On Windows PowerShell:
.\venv\Scripts\Activate.ps1

# On Linux/macOS:
# source venv/bin/activate

pip install -r requirements.txt


3. Start Application Services
Terminal 1 (FastAPI Producer API):
uvicorn app.main:app --reload --port 8000
Swagger UI docs available at http://127.0.0.1:8000/docs

Terminal 2 (Payment Processing Consumer Worker):
python app/consumer.py


📡 Sample API Request & Execution Flow
Endpoint: POST /process-payment
Request Payload:
{
  "transaction_id": "TXN-90210",
  "user_id": "USER-481",
  "amount": 349.99
}


API Response:
{
  "status": "SUCCESS",
  "message": "Transaction queued for processing",
  "data": {
    "transaction_id": "TXN-90210",
    "user_id": "USER-481",
    "amount": 349.99,
    "status": "PENDING"
  }
}


Consumer Worker Log Output:
[PROCESSED] Payment ID: TXN-90210 | Amount: $349.99 | Status: COMPLETED
[SKIPPED] Duplicate transaction detected: TXN-90210

# Kafka-fastapi-demo
A pet project for learning Apache Kafka and its integration with FastAPI. Implements an asynchronous event handling system, Dead Letter Queue (DLQ), and a topic control panel.

## Features
* **Producer:** Asynchronous message sending in Kafka.
* **Consumer Groups:** Consumer groups for scaling processing.
* **Event handling:** An example of business logic is sending emails.
* **Error handling:** Retry logic mechanism in case of failures.
* **Dead Letter Queue (DLQ):** Automatically move unsuccessfully processed messages to a separate topic.
* **Admin Client:** Topic management (creation, deletion) via code.
* **REST API:** Public endpoints on FastAPI for interacting with the system.
* **Manager:** A single point of management for Producer, ConsumerGroup and Admin client.

### Prerequisites
*   Python 3.10+
*   Docker and Docker Compose

### Installation & Running
1. **Clone the repository:**
```bash
    git clone https://github.com/KorzhakovNikita/kafka-fastapi-demo.git
```
2. **Create a virtual environment and install dependencies:**
```bash
    # for Linux/macOS
    python -m venv venv
    source venv/bin/activate  

    # for Windows
    .\venv\Scripts\activate
    pip install -r requirements.txt
```
  Create a `.env` file following the example of `.env.sample` and specify your settings.

3. **Launch Kafka (Docker):**
```bash
    docker-compose up -d
```
4. **Launch the app:**
```bash
    uvicorn main:app --reload
```
  * The application will be available at: http://localhost:8000
  *   Interactive Swagger UI: http://localhost:8000/docs

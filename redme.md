# 🚰 Microservices Water Tanks Tracker
Build as modern and scalable event-driven system with a **Python (FastAPI)** backend to manage IoT water tanks.
Orchestrate automated valve/pump controls, and dispatch multi-channel alerts (SMS/Email).

## 🏗️ Architecture & Core System Design

The system is built as an asynchronous **Event-Driven Architecture (EDA)**  on Apache Kafka.
Services have complete data isolation following the **Database-per-Service** pattern. 

### Core Data & Event Flow
    Service: front_api --------------------------> [action saved in tabase: PostgreSQL] -------------------------------------
  (Async Fast API)                                                  |                                                       |
        ▲                                                           ▼                                                       |
        |                                                 Service: DB_to_kafka                                              |
        |                                                  (event extraction)                                               |(are messeges enabled?)
        |               (topic:water_tank_power)                    |                   (topic:notification_service)        |
        |                   ---------------------------------------------------------------------------------               |
        |                   |                                                                                |              |
        |                   |                                                                                |              |
        |                   ▼                                                                                ▼              |
        |               Service:                                                                           Service: <--------
Action <----------   TurnOnWaterTank                       ---------------------------------------->  Notification Center
(Mock)                      |                              |                                   (waiting for front_api_return_msg) <--------> Database: Redis 
        |                   |                              |                                                  |                      (temp storage to match events)
        |                   |                              |                                                  |                            
        |                   |                              |                                                  |                                                                
        |                   |                              |                                                  |                                          
        |<----- (topic:front_api_return_msg)----------------                                     (topic:sms_service.message)                                       
        |                                                                                                     |
        |                                                                                                     ▼                                           
        |                                                                                                  Service: 
        |                                                                                                  Send_SMS  ----------> Action 
        |                           (topic:front_api_message_service)                                         |                  (Mock) 
        -------------------------------------------------------------------------------------------------------                                        
                                                                                                                                                         
                
---

## 🛠️ Microservices Breakdown

| Service Component | Purpose & Architectural Pattern | Tech Stack |
| **`app/` (Core API)** | Entry point. Exposes REST API for tank management, configuration, and event tracking. | FastAPI, Pydantic, ASGI |
| **`ServiceDBtoKafka`** | Database event extractor. Watches updates and pushes atomic messages to Kafka. | Python, Kafka Producer, SQL |
| **`ServiceTurnOnWaterTank`**| Device control worker. Listens to power events, toggles valves, and reports states back. | Python, Custom Generic Template |
| **`ServiceNotificationCenter`**| Intelligent router. Manages alert workflows, filters flags, and integrates with **Redis**. | Python, Redis, Kafka Consumer |
| **`ServiceSMS`** | High-priority alerting worker simulating telecom network gateways. | Python, Worker Pattern |

---

## 🚀 Advanced Engineering Features Implemented

*   **Generic Microservice Templating (Stage 17):** Standardized generic service template introducing a highly reusable generic template core (`main_logic`, `settings/kafka_in_out`), decoupling infrastructural Kafka patterns from custom business rules.
*   **Message Contract Unification (Stage 13-15):** Unified data structures using Pydantic contracts by building a generic Pydantic validator for kafka event and passed data. Allow frictionless data exchanges between components without drifting payload signatures.
*   **State Tracking & Correlation:** Integrated distributed correlation tracking mechanism (`corr_id` for tracking specific tank lifecycle state and, `action_id`for tracing distinct system events). 
*   **Infrastructure Isolation:** Multi-container orchestration config featuring custom environment injections (`POSTGRES_DB_LOCALHOST`) making environmental configuration seamless.
*   **Asynchronous Event Aggregation via Redis:** Utilized Redis as a high-performance transient state store within the Notification Center to synchronize asynchronous Kafka events. When a tank event triggers, the service buffers the context in Redis, waiting for the corresponding service lifecycle confirmation message. Once matched via correlation IDs and event IDs, the system evaluates user notification preferences (SMS/Email toggles) and orchestrates downstream alerting workflows, preventing race conditions.

## 🧪 Advanced Automated Test Suite (Testing Pyramid)

Quality assurance features a strict **Zero-Mock Policy** for system integration. The test suite (e.g., `test_kafka_e2e_sms_service.py`) utilizes real asynchronous consumers (`AioKafkaConsumer`) and producers (`AioKafkaProducer`) to validate full data round-trips over live network sockets, verifying that microservices correctly serialize, stream, and consume real payloads while strictly enforcing Pydantic data contracts.

The test suite is divided into two execution layers (Stage 19) to optimize performance and environments:

### 1. Local Testing & Execution
Local tests focus on isolated business logic and component integrations. Since the integration tests interact with real infrastructure, you must spin up the required containers before running the suite locally:

```bash
# Step 1: Spin up the infrastructure in the background
docker-compose up -d

# Step 2: Run the local test suite
pytest
```

### 2. Cloud CI/CD Execution & Pipeline E2E (Cascading Contracts)
The exact same zero-mock philosophy is automated in the cloud via **GitHub Actions**. The pipeline is split into isolated workflows that implement **sequential dependency testing**. 

Instead of relying on hardcoded scripts, the pipeline validates end-to-end integration by dynamically passing the verified output payloads of upstream microservices directly as the input triggers for downstream components.
* Every Pull Request or push to `main` branches automatically provisions the pipeline and executes the full architecture verification.

---

## 🚦 Quick Start (Local Docker Orchestration)

Spin up the entire microservices mesh (Core Services, Kafka Broker, Zookeeper, Redis cache, and PostgreSQL instances) with a single command:

```bash
docker-compose up --build
```

---

## 📝 Roadmap & Active Project TODOs
- [ ] **Telemetry Ingestion Expansion:** Implement dedicated mocked sensor microservices linked to tanks to provide continuous, real-time data streaming over Kafka.
- [ ] **Dynamic Container Orchestration:** Implement automated, on-demand spawning of isolated sensor containers (e.g., depth-sensors) triggered dynamically by user configurations via the API.
- [ ] **Frontend Implementation:** Build the 1st (Login) and 2nd (Tank Management Interface) screens to provide a unified visual layout for tracking active telemetry.
- [ ] **Testcontainers Integration:** Migrate local dependency on external Docker Compose infrastructure to a self-contained `testcontainers-python` setup within GitHub Actions pipelines.
- [ ] **Configuration Migration:** Refactor plain-text authentication tokens into strict structural JSON stores or environment secret managers.
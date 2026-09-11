```markdown
# Resilient Event-Driven Microservices Platform

A production-grade, fault-tolerant event-driven microservices ecosystem built with **Java 21**, **Spring Boot 4.x**, **NATS JetStream**, and **PostgreSQL**. Designed with persistent idempotency guarantees, dead-letter/poison-pill isolation, and edge reverse-proxy routing to fulfill all requirements for the Trams backend engineering evaluation.

---

## 1. System Architecture & Component Overview

```text
       [ External Client / Postman / curl ]
                         │
                         │ HTTP POST /api/v1/users/register
                         ▼
             ┌───────────────────────┐
             │      API GATEWAY      │ (Port 8080)
             │ Spring Cloud Gateway  │ - Non-blocking Netty reactive engine
             └───────────┬───────────┘ - Edge header injection (`X-Gateway-Processed`)
                         │
                         │ Internal Proxy Route
                         ▼
             ┌───────────────────────┐
             │     USER SERVICE      │ (Port 8081)
             │   Spring Boot 4.x     │ - Validates payload & saves to PostgreSQL
             └─────┬───────────┬─────┘ - Publishes domain events securely via NATS JetStream
                   │           │
       PostgreSQL  │           │ Publish: `user.events.created`
       Persistence │           │ Stream:  `USERS_STREAM` (File-backed durability)
                   ▼           ▼
        ┌─────────────┐   ┌───────────────────────────┐
        │ POSTGRESQL  │   │      NATS JETSTREAM       │ (Port 4222)
        │ Table:      │   │ - Persistent disk storage │
        │ `users`     │   │ - Token-authenticated     │
        └─────────────>   └─────────────┬─────────────┘
                                        │
                                        │ Durable Pull/Push Subscription
                                        │ Consumer: `notification-service-consumer`
                                        ▼
                          ┌───────────────────────────┐
                          │   NOTIFICATION SERVICE    │ (Port 8082)
                          │ - Persistent Idempotency  │
                          │ - Poison Pill Protection  │
                          │ - Explicit Manual ACK/NAK │
                          └─────────────┬─────────────┘
                                        │
                                        │ Verify & record eventId
                                        ▼
                                 ┌─────────────┐
                                 │ POSTGRESQL  │
                                 │ Table:      │
                                 │ `processed_ │
                                 │   events`   │
                                 └─────────────┘

```

---

## 2. Core Architectural & Resiliency Fulfillment

### A. Asynchronous, Broker-Based Communication (No REST/WebSockets)

In compliance with the assignment instructions, `user-service` and `notification-service` do not communicate via synchronous REST APIs or WebSockets. Instead, user registration triggers a domain event (`UserCreatedEvent`) published asynchronously to a disk-backed NATS JetStream broker (`USERS_STREAM`). The API Gateway handles ingress and proxies to `user-service`, which returns `201 Created` in sub-50ms without waiting for downstream worker processing.

### B. Secure & Reliable Messaging

* **Broker Security:** NATS is configured with strict token-based authentication (`--auth`). Any publisher or consumer lacking the pre-shared secret token (`super_secure_nats_token_2026`) is rejected at the connection handshake.
* **Durable Offsets:** The notification worker subscribes as a named durable consumer (`notification-service-consumer`). If the worker crashes or goes offline, unacknowledged messages are retained on disk by NATS and replayed sequentially upon recovery.

### C. Persistent Idempotency Deduplication

To handle at-least-once delivery guarantees safely without duplicate actions:

* Every event carries an immutable UUID `eventId`.
* The `notification-service` checks PostgreSQL's `processed_events` table before executing side-effects.
* If an event ID already exists, it logs a warning, skips side-effects, and immediately sends an explicit `ACK` to NATS to advance the stream offset.

### D. Poison-Pill Protection & Error Handling

Malformed payloads (corrupted JSON) are isolated safely. Rather than entering an infinite `NAK` redelivery loop, the consumer catches deserialization exceptions, logs them as unrecoverable `[POISON PILL]` events, and issues an explicit `ACK` to discard the payload from the queue.

---

## 3. Tech Stack & Environment Configuration

* **Runtime:** Java 21 (Eclipse Temurin)
* **Framework:** Spring Boot 4.1.1, Spring Data JPA
* **Edge Proxy:** Spring Cloud Gateway Server WebFlux (Port 8080)
* **Messaging:** NATS JetStream 2.10 (Token Authenticated)
* **Database:** PostgreSQL 16 (Configured via Docker environment variables)

---

## 4. API Documentation

### Register User (API Gateway Endpoint)

* **URL:** `POST http://localhost:8080/api/v1/users/register`
* **Content-Type:** `application/json`
* **Request Body:**
```json
{
  "fullName": "Aarav Sharma",
  "email": "aarav@example.com"
}

```


* **Success Response (`201 Created`):**
```json
{
  "id": 1,
  "fullName": "Aarav Sharma",
  "email": "aarav@example.com",
  "createdAt": "2026-09-11T15:30:00.123456Z"
}

```



---

## 5. Local Setup & Execution Instructions

### Prerequisites

* Docker and Docker Compose installed on your machine.

### Step 1: Clone and Start the Platform

Navigate to the root directory containing `docker-compose.yml` and run:

```bash
docker compose up --build -d

```

### Step 2: Verify Container Health

Run the following command to check that all 5 infrastructure and application containers (`nats-broker`, `postgres-db`, `user-service`, `notification-service`, `api-gateway`) are running and healthy:

```bash
docker compose ps

```

### Step 3: Test the Live Event Flow

Execute a registration request:

```bash
curl -X POST http://localhost:8080/api/v1/users/register \
  -H "Content-Type: application/json" \
  -d '{"fullName": "Arjun Mehta", "email": "arjun@example.com"}'

```

Watch the background asynchronous workflow execute in the notification service logs:

```bash
docker logs -f notification-service

```

```text
DISPATCHING NOTIFICATION:
  Recipient : Arjun Mehta <arjun@example.com>
  User ID   : 1
  Message   : Welcome to Trams, Arjun Mehta! Your registration is complete.
  Event ID  : ...
[ACKNOWLEDGED] Event [...] committed successfully

```

```

```

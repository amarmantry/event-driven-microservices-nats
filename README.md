Markdown
# Event-Driven Microservices Platform

An event-driven microservices system built with Java 21, Spring Boot, NATS JetStream, and PostgreSQL.

The system demonstrates asynchronous communication between a User Service and Notification Service through NATS JetStream, with durable messaging, explicit acknowledgements, validation, persistent event tracking, and Docker-based local deployment.

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
       PostgreSQL │           │ Publish: `user.events.created`
       Persistence│           │ Stream:  `USERS_STREAM` (File-backed durability)
                  ▼           ▼
        ┌─────────────┐   ┌───────────────────────────┐
        │ POSTGRESQL  │   │      NATS JETSTREAM       │ (Port 4222)
        │ Table:      │   │ - Persistent disk storage │
        │ `users`     │   │ - Token-authenticated     │
        └─────────────┘   └─────────────┬─────────────┘
                                        │
                                        │ Durable Push Subscription
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
2. Core Architectural & Resiliency Fulfillment
A. Asynchronous, Broker-Based Communication (No REST/WebSockets)
In compliance with the assignment instructions, user-service and notification-service do not communicate via synchronous REST APIs or WebSockets. Instead, user registration triggers a UserCreatedEvent, which is published to a disk-backed NATS JetStream broker (USERS_STREAM). The User Service waits for the broker to acknowledge the publish, but does not wait for the Notification Service to process the event.

B. Secure & Reliable Messaging
Broker Security: NATS is configured with token-based authentication. Publishers and consumers must provide the configured NATS token to establish a connection with the broker.

Durable Messaging: The Notification Service uses a named durable consumer (notification-service-consumer). If the service goes offline, unacknowledged messages remain available in the JetStream stream and can be redelivered when the consumer recovers.

Explicit Acknowledgements: Messages are explicitly acknowledged after successful processing. Failed processing results in a negative acknowledgement, allowing NATS to redeliver the message.

Configuration & Secrets: Database credentials, service URLs, and broker configuration are supplied through environment variables. Secrets should be managed through a secure secret-management solution in a production environment and should not be committed to source control.

Service Communication: The User Service and Notification Service communicate asynchronously through NATS JetStream rather than directly through REST or WebSockets.

C. Persistent Event Tracking and Duplicate Handling
Each event contains a unique eventId. The Notification Service stores processed event IDs in PostgreSQL (processed_events table). If an already-processed event is delivered again (e.g., due to network retries), the service detects the existing event ID, skips the notification logic, and acknowledges the message.

D. Poison-Pill Protection & Error Handling
Malformed payloads (corrupted JSON) are isolated safely. Rather than entering an infinite NAK redelivery loop, the consumer catches deserialization exceptions, logs them as unrecoverable [POISON PILL] events, and issues an explicit ACK to discard the payload from the queue.

3. Tech Stack & Environment Configuration
Runtime: Java 21 (Eclipse Temurin)

Framework: Spring Boot 4.1.1, Spring Data JPA

Edge Proxy: Spring Cloud Gateway Server WebFlux (Port 8080)

Messaging: NATS 2.10 with JetStream (Token Authenticated)

Database: PostgreSQL 16 (Configured via Docker environment variables)

4. API Documentation
Register User (API Gateway Endpoint)
URL: POST http://localhost:8080/api/v1/users/register

Content-Type: application/json

Request Body:

JSON
{
  "fullName": "Aarav Sharma",
  "email": "aarav@example.com"
}
Success Response (201 Created):

JSON
{
  "id": 1,
  "fullName": "Aarav Sharma",
  "email": "aarav@example.com",
  "createdAt": "2026-09-11T15:30:00.123456Z"
}
5. Local Setup & Execution Instructions
Prerequisites
Docker and Docker Compose installed on your machine.

Step 1: Clone and Start the Platform
Navigate to the root directory containing docker-compose.yml and run:

Bash
docker compose up --build -d
Step 2: Verify Containers
Run:

Bash
docker compose ps
Verify that nats-broker, postgres-db, user-service, notification-service, and api-gateway are running and healthy.

Step 3: Test the Live Event Flow
Execute a registration request:

Bash
curl -X POST http://localhost:8080/api/v1/users/register \
  -H "Content-Type: application/json" \
  -d '{"fullName": "Arjun Mehta", "email": "arjun@example.com"}'
Watch the background asynchronous workflow execute in the notification service logs:

Bash
docker logs -f notification-service
Plaintext
DISPATCHING NOTIFICATION:
  Recipient : Arjun Mehta <arjun@example.com>
  User ID   : 1
  Message   : Welcome to Trams, Arjun Mehta! Your registration is complete.
  Event ID  : ...
[ACKNOWLEDGED] Event [...] committed successfully

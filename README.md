# ledger-query-service

The **read side** of a CQRS/Event-Sourcing ledger system built with **Spring Boot 3.4 + Java 21**.

It consumes domain events from **Apache Kafka**, projects them into **PostgreSQL** read-model tables, caches balances in **Redis**, and exposes a REST API documented via **OpenAPI / Swagger UI**.

---

## Architecture overview

```
┌──────────────────────┐       ledger.events topic        ┌──────────────────────────┐
│  ledger-command-side │ ──────────────────────────────►  │  LedgerEventConsumer     │
│  (write side)        │                                   │  (Kafka listener)        │
└──────────────────────┘                                   └──────────┬───────────────┘
                                                                       │  projects events
                                                           ┌───────────▼───────────────┐
                                                           │  PostgreSQL (read-model)  │
                                                           │  • account_summary        │
                                                           │  • account_balances       │
                                                           │  • transaction_history    │
                                                           │  • event_log              │
                                                           └───────────┬───────────────┘
                                                                       │
                                                           ┌───────────▼───────────────┐
                                                           │  LedgerQueryController   │
                                                           │  REST API  :8081         │
                                                           └───────────┬───────────────┘
                                                                       │  balance cache
                                                           ┌───────────▼───────────────┐
                                                           │  Redis                    │
                                                           └───────────────────────────┘
```

---

## Tech stack

| Layer | Technology |
|---|---|
| Language | Java 21 |
| Framework | Spring Boot 3.4.5 |
| Messaging | Apache Kafka (spring-kafka) |
| Database | PostgreSQL + Flyway migrations |
| Cache | Redis (spring-data-redis) |
| API docs | SpringDoc OpenAPI / Swagger UI |
| Build | Maven (Maven Wrapper) |
| Container | Docker (multi-stage, eclipse-temurin:21) |

---

## Event types handled

| Event | Projection effect |
|---|---|
| `ACCOUNT_CREATED` | Inserts into `account_summary` and `account_balances` |
| `ACCOUNT_CREDITED` | Updates balance +, inserts into `transaction_history` |
| `ACCOUNT_DEBITED` | Updates balance −, inserts into `transaction_history` |
| `ACCOUNT_STATUS_CHANGED` | Updates `account_summary.status` |
| `TRANSFER_INITIATED` | Records transfer legs in both involved accounts |
| `TRANSFER_REVERSED` | Reverses the original transfer entries |

All projections are **idempotent** — duplicate events are ignored via the `last_event_id` check on `account_balances`.

---

## REST API

Base path: `/api/v1`  
Interactive docs: [`http://localhost:8081/swagger-ui/index.html`](http://localhost:8081/swagger-ui/index.html)

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/accounts/{accountId}/balance` | Current balance (Redis cache-first) |
| `GET` | `/accounts/{accountId}/transactions` | Paginated transaction history with optional `from`, `to`, `direction` filters |
| `GET` | `/accounts/{accountId}/statement?month=YYYY-MM` | Monthly statement with opening & closing balance |
| `GET` | `/accounts/{accountId}/events` | Full raw event audit trail |
| `GET` | `/accounts?ownerId={ownerId}` | All accounts for an owner |
| `POST` | `/admin/replay/{accountId}` | Recompute balance from `transaction_history` (recovery) |
| `GET` | `/health/lag` | Kafka consumer group lag per partition |

---

## Configuration

All values can be overridden via environment variables:

| Property | Env Variable | Default |
|---|---|---|
| Server port | — | `8081` |
| DB host | `QUERY_DB_HOST` | `localhost` |
| DB port | `QUERY_DB_PORT` | `5432` |
| DB username | `QUERY_DB_USERNAME` | `ledger_query` |
| DB password | `QUERY_DB_PASSWORD` | `ledger_query` |
| Kafka bootstrap | `LEDGER_KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| Redis host | `LEDGER_REDIS_HOST` | `localhost` |
| Redis port | `LEDGER_REDIS_PORT` | `6379` |

Kafka topic consumed: `ledger.events`  
Consumer group: `ledger-query-projector`

---

## Database schema

Flyway applies migrations from `src/main/resources/db/migration/`.

**`V1__create_query_tables.sql`** creates:

- `account_summary` — account metadata and status
- `account_balances` — current balance per account
- `transaction_history` — all debit/credit entries
- `event_log` — raw event payloads (full audit trail)

---

## Running locally

### Prerequisites

- Java 21+
- PostgreSQL running with database `ledger_query`
- Redis running on `localhost:6379`
- Kafka broker on `localhost:9092`

### Run

```bash
./mvnw spring-boot:run
```

Or with explicit env vars:

```bash
QUERY_DB_HOST=localhost \
QUERY_DB_PASSWORD=secret \
LEDGER_KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
./mvnw spring-boot:run
```

### Run tests

```bash
./mvnw test
```

All 31 tests run without a real database, Redis, or Kafka instance — everything is mocked.

---

## Docker

### Build image

```bash
docker build -t ledger-query-service .
```

### Run container

```bash
docker run -p 8081:8081 \
  -e QUERY_DB_HOST=host.docker.internal \
  -e QUERY_DB_PASSWORD=secret \
  -e LEDGER_KAFKA_BOOTSTRAP_SERVERS=host.docker.internal:9092 \
  -e LEDGER_REDIS_HOST=host.docker.internal \
  ledger-query-service
```

---

## Project structure

```
src/
├── main/
│   ├── java/com/marioreis/ledgerquery/
│   │   ├── api/            # REST controllers, DTOs, exception handler
│   │   ├── config/         # Kafka, Redis, Swagger configuration
│   │   ├── domain/         # LedgerEventType enum
│   │   ├── persistence/    # ProjectionRepository (JDBC)
│   │   ├── projection/     # LedgerEventConsumer (Kafka listener)
│   │   └── service/        # BalanceCacheService, KafkaLagService
│   └── resources/
│       ├── application.properties
│       └── db/migration/   # Flyway SQL scripts
└── test/
    └── java/com/marioreis/ledgerquery/
        ├── api/            # LedgerQueryControllerTest
        ├── projection/     # LedgerEventConsumerTest
        └── service/        # BalanceCacheServiceTest
```

---

## License

MIT


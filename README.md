# FleetPulse

FleetPulse is a software-only connected-vehicle intelligence platform for fleet managers. It will eventually process telemetry from 100,000+ virtual vehicles to identify abnormal behavior, explain vehicle risk, and recommend maintenance actions.

No physical IoT hardware is required: vehicles, telemetry, faults, and maintenance outcomes will be simulated in software.

## Current status

Phase 0 established the repository foundation and local PostgreSQL, Redis, and Kafka infrastructure. Phase 1 added the transactional PostgreSQL core and read-oriented domain APIs. Phase 2 adds a software-only simulator and shared telemetry contract. Phase 3 adds durable Kafka ingestion with Redis-backed application idempotency. Phase 4 adds real-time rule-based stream processing and PostgreSQL-backed anomaly alerts. Block 1 adds ClickHouse historical telemetry analytics and a Python predictive-maintenance risk service.

## Architecture

The API is the initial application boundary. PostgreSQL is reserved for transactional domain data, Redis provides idempotency state, and Kafka is the durable high-volume telemetry stream. See [the architecture document](docs/architecture/architecture.md) and [ADRs](docs/adr).

## Repository structure

`apps/api` contains the Node.js/TypeScript API. `apps/web` and future services are reserved as boundaries, while shared configuration lives in `packages/config`. Infrastructure and documentation live under `infrastructure`, `docs`, and the root Compose file.

`services/telemetry-simulator` contains the bounded in-memory simulator. `services/ingestion` contains the shared Kafka producer boundary and ingestion consumer. `services/stream-processor` contains bounded event-time state, explainable anomaly detectors, cooldowns, and alert persistence. `packages/schemas` contains the versioned telemetry event contract shared by all services.

`services/historical-analytics` consumes Kafka independently into ClickHouse and exposes historical query/export commands. `services/ml-service` contains leakage-safe feature engineering, chronological training, baseline/improved models, and a FastAPI risk-estimate endpoint.

## Prerequisites

- Node.js 22+
- npm 10+
- Docker Desktop with Compose v2

## Local setup

```powershell
Copy-Item .env.example .env
npm install
docker compose up -d
```

The Compose services communicate by service name inside Docker. The host-facing development endpoints are PostgreSQL on `localhost:55432`, Redis on `localhost:6379`, and Kafka on `localhost:9092`. Inside Docker, PostgreSQL remains available as `postgres:5432`.
ClickHouse is optional for the existing stack and is available on `localhost:8123` with `docker compose up -d clickhouse`.

## API

```powershell
npm run dev
Invoke-RestMethod http://localhost:3000/health
```

The API returns a structured liveness response containing its service name, timestamp, and request ID.

## Connectivity checks

```powershell
docker compose ps
docker compose exec postgres pg_isready -U fleetpulse -d fleetpulse
docker compose exec redis redis-cli ping
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic phase0-smoke --partitions 1 --replication-factor 1
docker compose exec -T kafka sh -c "printf 'phase0-message\n' | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic phase0-smoke"
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic phase0-smoke --from-beginning --timeout-ms 5000
```

## Development commands

```powershell
npm run typecheck
npm run lint
npm test
npm run build
npm run format:check
npm run simulator -- --vehicles 1000 --duration 10 --rate 100
npm run simulator -- --vehicles 1000 --duration 10 --rate 100 --output ./tmp/events.jsonl
npm run ingestion
npm run stream-processor
npm run simulator -- --vehicles 10 --duration 10 --rate 10 --output-mode kafka
```

Stop local infrastructure with `docker compose down`; add `-v` when intentionally removing local database, Redis, and Kafka volumes.

## Technology direction

The current foundation uses Node.js, TypeScript, Fastify, Zod, Pino, PostgreSQL, Redis, Kafka, Docker Compose, ESLint, Prettier, and Vitest. Future analytical storage, ML, AI, observability, and orchestration choices will be introduced only in their corresponding phases.

## Codebase visualization

A code-only Graphify visualization of the current repository is available at [docs/graphify/graphify-out/graph.html](docs/graphify/graphify-out/graph.html). The accompanying graph data and extraction manifest are stored beside it.

## Phase 2 simulator

The simulator represents virtual vehicles only; no physical IoT hardware is involved. It uses shared schema version `1.0`, correlated vehicle state, configurable regions, burst traffic, duplicate events, out-of-order delivery, bounded output buffering, and explainable fault scenarios. See [the simulator guide](services/telemetry-simulator/README.md).

## Phase 3 Kafka ingestion

Start the existing Kafka and Redis services with `docker compose up -d kafka redis`, then run `npm run ingestion` in one terminal. In another, run `npm run simulator -- --vehicles 10 --duration 10 --rate 10 --output-mode kafka`.

Telemetry is published as JSON to `vehicle.telemetry.v1`, keyed by canonical `vehicleId`, with `schema-version` and `event-type` headers. The topic defaults to six partitions and seven-day development retention. The ingestion consumer uses group `fleetpulse-ingestion`, validates messages, and applies Redis `SET NX EX` under `telemetry:idempotency:{eventId}`. Kafka remains an at-least-once delivery log; application processing is idempotent, not globally exactly-once.

Kafka retains messages for replay. Start a new configurable consumer group with `KAFKA_GROUP_ID` and `KAFKA_FROM_BEGINNING=true` to independently replay retained events. Out-of-order timestamps with distinct event IDs remain distinct and are not reordered in this phase. Consumer processing is bounded by KafkaJS per-partition handling, so Kafka retains backlog instead of an unbounded application queue or silent drops.

## Phase 4 stream processing

Run `npm run stream-processor` after PostgreSQL is migrated/seeded and Kafka/Redis are running. The dedicated `fleetpulse-stream-processor` group validates events, keeps bounded event-time windows, and evaluates overheating, rapid temperature rise, harsh braking, excessive idling, low SOC, low SoH, and critical-fault rules. Only detections are written to the existing `Alert` model. Redis provides event idempotency and alert cooldown TTLs; repeated anomaly readings are suppressed and alerts resolve after normal telemetry. See [the stream processor guide](services/stream-processor/README.md) and [ADR-005](docs/adr/ADR-005-real-time-rule-stream-processing.md).

## Block 2 product APIs and dashboard

The API exposes dashboard, filtered vehicle/alert, telemetry-summary, analytics, and maintenance-risk routes. Start the web dashboard with `npm run dev --workspace=@fleetpulse/web`; set `NEXT_PUBLIC_API_BASE_URL` when the API is not on `http://localhost:3000`. The controlled assistant is available at `POST /api/v1/agent/query` and uses only registered product tools; its deterministic fallback keeps the core product independent of an external model or vector store.

## Block 1 historical analytics and ML

Start ClickHouse with `docker compose up -d clickhouse`, then run `npm run historical-analytics` with Kafka available. It uses the separate `fleetpulse-historical-analytics` group and stores typed telemetry in a date-partitioned ClickHouse MergeTree table ordered by vehicle and event time. Export raw historical rows with `npm run historical-export` before running the Python training command in [services/ml-service](services/ml-service/README.md). The ML service predicts a seven-day maintenance-risk estimate only; it does not control vehicles or claim certainty. ClickHouse is the analytical store; PostgreSQL remains transactional and does not receive the telemetry firehose.

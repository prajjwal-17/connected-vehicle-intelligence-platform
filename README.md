# FleetPulse

FleetPulse is a software-only connected-vehicle intelligence platform for fleet managers. It will eventually process telemetry from 100,000+ virtual vehicles to identify abnormal behavior, explain vehicle risk, and recommend maintenance actions.

No physical IoT hardware is required: vehicles, telemetry, faults, and maintenance outcomes will be simulated in software.

## Current status

Phase 0 established the repository foundation and local PostgreSQL, Redis, and Kafka infrastructure. Phase 1 added the transactional PostgreSQL core and read-oriented domain APIs. Phase 2 adds a software-only simulator and shared telemetry contract. Kafka ingestion, stream processing, ML, AI, authentication, and frontend functionality remain future phases.

## Architecture

The API is the initial application boundary. PostgreSQL is reserved for transactional domain data, Redis for ephemeral state and caching, and Kafka for the future high-volume telemetry stream. See [the architecture document](docs/architecture/architecture.md) and [ADRs](docs/adr).

## Repository structure

`apps/api` contains the Node.js/TypeScript API. `apps/web` and future services are reserved as boundaries, while shared configuration lives in `packages/config`. Infrastructure and documentation live under `infrastructure`, `docs`, and the root Compose file.

`services/telemetry-simulator` contains the bounded in-memory simulator. `packages/schemas` contains the versioned telemetry event contract shared with future ingestion services.

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
```

Stop local infrastructure with `docker compose down`; add `-v` when intentionally removing local database, Redis, and Kafka volumes.

## Technology direction

The current foundation uses Node.js, TypeScript, Fastify, Zod, Pino, PostgreSQL, Redis, Kafka, Docker Compose, ESLint, Prettier, and Vitest. Future analytical storage, ML, AI, observability, and orchestration choices will be introduced only in their corresponding phases.

## Codebase visualization

A code-only Graphify visualization of the current repository is available at [docs/graphify/graphify-out/graph.html](docs/graphify/graphify-out/graph.html). The accompanying graph data and extraction manifest are stored beside it.

## Phase 2 simulator

The simulator represents virtual vehicles only; no physical IoT hardware is involved. It uses shared schema version `1.0`, correlated vehicle state, configurable regions, burst traffic, duplicate events, out-of-order delivery, bounded output buffering, and explainable fault scenarios. See [the simulator guide](services/telemetry-simulator/README.md). Kafka output is intentionally not implemented until Phase 3.

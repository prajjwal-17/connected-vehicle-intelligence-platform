# FleetPulse architecture

## Purpose

FleetPulse will help fleet managers understand which software-simulated vehicles show abnormal behavior or may need maintenance soon, why they are at risk, and what action to take.

## Current Phase 0 flow

The API exposes liveness information. PostgreSQL, Redis, and Kafka run as isolated local infrastructure services. No vehicle telemetry or domain data is processed yet.

## Intended future flow

Virtual vehicles will emit correlated synthetic telemetry into Kafka. Future ingestion and stream-processing services will validate, deduplicate, aggregate, and detect anomalies before persisting appropriate results and publishing alerts. The API and future web application will read operational and fleet views. A future Python ML service will consume engineered historical data and return maintenance risk scores. A future controlled AI agent may use authorized fleet tools and curated documentation.

## Component boundaries

- **API:** transactional and query boundary; currently only `/health`.
- **Kafka:** future durable telemetry stream with partitions, replay, and consumer groups.
- **PostgreSQL:** transactional domain data, configuration, users, alerts, and audit records in future phases; raw high-volume telemetry will not primarily live here.
- **Redis:** future cache, ephemeral vehicle state, coordination, and rate-limiting state.
- **Analytical storage:** future ClickHouse and/or object storage decision, justified when historical telemetry workloads are designed.
- **ML service:** future Python service for leakage-safe predictive maintenance experiments and serving.
- **AI agent:** future controlled tool-using fleet assistant with authorization and prompt-injection safeguards.

## Scalability principles

Future designs will use partitioning, idempotent processing, at-least-once delivery, back-pressure, retries, dead-letter handling, bounded APIs, tenant isolation, and measured capacity. Scale targets are claims only after benchmark evidence exists.

# FleetPulse architecture

## Purpose

FleetPulse will help fleet managers understand which software-simulated vehicles show abnormal behavior or may need maintenance soon, why they are at risk, and what action to take.

## Current Phase 2 flow

The API and transactional PostgreSQL core coexist with a software-only telemetry simulator. The simulator initializes lightweight in-memory vehicle state, advances it in a shared loop, validates versioned events, and emits them to stdout or JSONL through a bounded sink. Kafka ingestion is not part of this phase.

## Intended future flow

Virtual vehicles currently emit correlated synthetic telemetry to local sinks. In a future phase those events will enter Kafka. Future ingestion and stream-processing services will validate, deduplicate, aggregate, and detect anomalies before persisting appropriate results and publishing alerts. The API and future web application will read operational and fleet views. A future Python ML service will consume engineered historical data and return maintenance risk scores. A future controlled AI agent may use authorized fleet tools and curated documentation.

## Component boundaries

- **API:** transactional and query boundary; currently only `/health`.
- **Kafka:** future durable telemetry stream with partitions, replay, and consumer groups.
- **PostgreSQL:** transactional domain data, configuration, users, alerts, and audit records in future phases; raw high-volume telemetry will not primarily live here.
- **Redis:** future cache, ephemeral vehicle state, coordination, and rate-limiting state.
- **Telemetry simulator:** Phase 2 software-only vehicle population and event generator. It uses shared state objects rather than one process, worker, or timer per vehicle.
- **Telemetry contract:** versioned Zod schema in `packages/schemas`, including event identity, event time, delivery time, sequence number, motion, engine, fuel, battery, and fault signals.
- **Analytical storage:** future ClickHouse and/or object storage decision, justified when historical telemetry workloads are designed.
- **ML service:** future Python service for leakage-safe predictive maintenance experiments and serving.
- **AI agent:** future controlled tool-using fleet assistant with authorization and prompt-injection safeguards.

## Scalability principles

Future designs will use partitioning, idempotent processing, at-least-once delivery, back-pressure, retries, dead-letter handling, bounded APIs, tenant isolation, and measured capacity. Scale targets are claims only after benchmark evidence exists.

## Simulator and future ML data

The simulator deliberately generates correlated state transitions and explainable precursor patterns for engine overheating, battery degradation, brake degradation, and tire pressure issues. `eventId` is the future ingestion idempotency key; `timestamp` is original simulated event time, while `deliveryTimestamp` can differ when delayed delivery is enabled. These events plus future maintenance outcomes provide the raw material for a time-based predictive-maintenance dataset without implementing ML in this phase.

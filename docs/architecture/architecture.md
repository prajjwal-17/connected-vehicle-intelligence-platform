# FleetPulse architecture

## Purpose

FleetPulse will help fleet managers understand which software-simulated vehicles show abnormal behavior or may need maintenance soon, why they are at risk, and what action to take.

## Current Phase 3 flow

The API and transactional PostgreSQL core coexist with a software-only telemetry simulator. The simulator initializes lightweight in-memory vehicle state, advances it in a shared loop, validates versioned events, and emits them through a bounded sink. The Kafka sink publishes JSON events to `vehicle.telemetry.v1`, where a named ingestion consumer validates, deduplicates through Redis, and processes them.

## Telemetry ingestion flow

```text
Virtual Vehicles -> Telemetry Simulator -> Kafka Producer -> Kafka
                                      -> Ingestion Consumer -> Schema Validation
                                                           -> Redis Idempotency
                                                           -> Ingestion Processing
```

- Topic: `vehicle.telemetry.v1` (configurable)
- Default partitions: 6; development replication factor: 1; retention: 7 days
- Message key: `vehicleId`, preserving normal per-vehicle partition ordering
- Value: versioned JSON telemetry event; headers carry `schema-version` and `event-type`
- Consumer group: `fleetpulse-ingestion` (configurable; new groups can replay retained data)
- Delivery: Kafka producer uses acknowledgements from all in-sync replicas, retries, compression, and idempotent producer mode where supported by the broker. Consumer offsets are committed after `eachMessage` returns, after validation, Redis claim, and processing.
- Idempotency: Redis `SET NX EX` uses `telemetry:idempotency:{eventId}`. A processing failure releases the claim so a redelivery can retry. This is at-least-once-compatible, idempotent application processing; it is not a global exactly-once guarantee.
- Invalid messages are rejected and counted. Transient processing failures are rethrown so KafkaJS does not acknowledge the message. No DLQ is added yet because there is no permanent-error policy or operator workflow in this phase.
- Distinct event IDs with out-of-order timestamps are preserved as distinct events. Event-time reordering belongs to a later stream-processing phase.
- KafkaJS processes messages per partition without an unbounded application queue; Kafka retains backlog when processing cannot keep up.

## Intended future flow

Future stream-processing services will aggregate and detect anomalies before persisting appropriate results and publishing alerts. The API and future web application will read operational and fleet views. A future Python ML service will consume engineered historical data and return maintenance risk scores. A future controlled AI agent may use authorized fleet tools and curated documentation.

## Component boundaries

- **API:** transactional and query boundary; currently only `/health`.
- **Kafka:** Phase 3 durable telemetry stream with partitions, replay, and consumer groups.
- **PostgreSQL:** transactional domain data, configuration, users, alerts, and audit records in future phases; raw high-volume telemetry will not primarily live here.
- **Redis:** Phase 3 event-id idempotency state, with future cache, ephemeral vehicle state, coordination, and rate-limiting roles.
- **Telemetry simulator:** Phase 2 software-only vehicle population and event generator. It uses shared state objects rather than one process, worker, or timer per vehicle, and selects Kafka through the existing sink abstraction.
- **Ingestion:** Phase 3 shared Kafka producer boundary and consumer. It performs schema validation, Redis idempotency, controlled processing, and structured metrics/logging; it does not perform analytics or ML.
- **Telemetry contract:** versioned Zod schema in `packages/schemas`, including event identity, event time, delivery time, sequence number, motion, engine, fuel, battery, and fault signals.
- **Analytical storage:** future ClickHouse and/or object storage decision, justified when historical telemetry workloads are designed.
- **ML service:** future Python service for leakage-safe predictive maintenance experiments and serving.
- **AI agent:** future controlled tool-using fleet assistant with authorization and prompt-injection safeguards.

## Scalability principles

Future designs will use partitioning, idempotent processing, at-least-once delivery, back-pressure, retries, dead-letter handling, bounded APIs, tenant isolation, and measured capacity. Scale targets are claims only after benchmark evidence exists.

## Simulator and future ML data

The simulator deliberately generates correlated state transitions and explainable precursor patterns for engine overheating, battery degradation, brake degradation, and tire pressure issues. `eventId` is the future ingestion idempotency key; `timestamp` is original simulated event time, while `deliveryTimestamp` can differ when delayed delivery is enabled. These events plus future maintenance outcomes provide the raw material for a time-based predictive-maintenance dataset without implementing ML in this phase.

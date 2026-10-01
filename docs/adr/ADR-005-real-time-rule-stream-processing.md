# ADR-005: Real-Time Rule-Based Stream Processing

## Context

FleetPulse needs explainable anomaly signals within seconds before predictive-maintenance models exist. Phase 3 already provides a durable, replayable Kafka telemetry stream and Redis idempotency.

## Decision

Add a dedicated stream-processor consumer group. It validates the shared telemetry contract, maintains bounded event-time windows per vehicle, and applies independently testable deterministic rules for thermal, motion, idle, battery, and critical fault signals. Only detected alerts are persisted to the existing PostgreSQL `Alert` model. Redis stores bounded idempotency and cooldown keys with TTLs.

## Consequences

Rules are transparent and easy to test, but thresholds are synthetic and cannot predict failures. Kafka partitions and consumer instances provide the scaling path. PostgreSQL avoids the raw telemetry firehose, while alert persistence remains subject to bounded retries and Kafka redelivery. Event-time windows accept bounded lateness; full watermarks and advanced stream analytics remain future work.

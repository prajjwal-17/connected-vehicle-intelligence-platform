# ADR-006: ClickHouse for Historical Telemetry Analytics

## Context

FleetPulse now needs historical telemetry queries and feature extraction beyond the intended workload of the transactional PostgreSQL schema. Kafka remains the durable stream, while PostgreSQL owns fleets, vehicles, maintenance, alerts, users, and audit records.

## Decision

Use ClickHouse locally as a warm analytical store. A separate Kafka consumer group writes the shared telemetry event fields to a MergeTree table partitioned by `toDate(event_timestamp)` and ordered by `(vehicle_id, event_timestamp, event_id)`. JSON is retained at the Kafka boundary; the analytical table uses typed columns for filtering and aggregation. Local cold archival is documented as a future Parquet/object-storage tier.

## Alternatives considered

- PostgreSQL: already present and reliable for transactional data, but raw high-volume telemetry would compete with domain workloads and lacks the intended columnar compression/scan model.
- SQLite or local files: easy for demos but weak for concurrent historical analytics and horizontal growth.
- Cloud warehouse: inappropriate for a self-contained local hackathon stack.

## Consequences

ClickHouse adds one manageable local service and gives date partition pruning, typed column scans, and independent analytical scaling. It requires an additional operational dependency and a retention policy. The historical consumer is independent from Phase 3 ingestion and Phase 4 stream processing, so Kafka can replay the same events into each workflow.

# ADR-002: Kafka for the telemetry stream

## Decision

Kafka is the intended event backbone for high-volume telemetry.

## Rationale

Kafka provides durable ordered partitions, replay, consumer groups, and independent consumers. Those properties fit telemetry processing, anomaly detection, historical materialization, and future model-data generation. RabbitMQ is better suited to many task-queue and routing workloads, but Kafka's partitioned log model is a closer match for a replayable telemetry stream. This decision does not claim a performance result; capacity must be benchmarked.

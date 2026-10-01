# ADR-004: Kafka as Durable Telemetry Event Backbone

## Context

FleetPulse needs a durable boundary for high-volume synthetic vehicle telemetry. Phase 2 already produces versioned events, duplicates, delayed delivery, and bounded output. The next boundary must support partitioned ingestion, replay, and horizontal consumer scaling without coupling the simulator to Kafka internals.

## Decision

Use the existing Apache Kafka 4.0.0 KRaft service and Redis service from Docker Compose. Publish versioned JSON events to configurable topic `vehicle.telemetry.v1`, keyed by canonical `vehicleId`. Use a shared KafkaJS producer and a named consumer group. Use Redis `SET NX EX` for application-level idempotency by `eventId`.

The delivery model is Kafka at-least-once-compatible with idempotent application processing. This is not a global exactly-once claim. JSON is deliberate for Phase 3 interoperability and debuggability; Avro/Protobuf/Schema Registry can be evaluated after measured serialization and governance needs exist.

## Alternatives considered

- Direct HTTP ingestion: simpler initially, but does not provide the same durable partitioned replay log.
- Redis Streams: already available, but Kafka better matches the intended high-volume partition and independent consumer-group model.
- RabbitMQ: strong work-queue semantics, but Kafka's retained log and replay model fit telemetry history better.

## Consequences

Kafka provides replay, vehicle-based partitioning, and horizontal scaling by adding partitions and consumer instances. It also adds broker operations and requires explicit application idempotency because redelivery remains possible. Redis is an additional dependency for the idempotency claim and TTL lifecycle. Out-of-order event-time handling and analytics remain intentionally deferred to later phases.

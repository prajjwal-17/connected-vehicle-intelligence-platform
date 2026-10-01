# FleetPulse architecture

## Purpose

FleetPulse will help fleet managers understand which software-simulated vehicles show abnormal behavior or may need maintenance soon, why they are at risk, and what action to take.

## Current Block 1 flow

The API and transactional PostgreSQL core coexist with a software-only telemetry simulator. The simulator initializes lightweight in-memory vehicle state, advances it in a shared loop, validates versioned events, and emits them through a bounded sink. The Kafka sink publishes JSON events to `vehicle.telemetry.v1`, where independent ingestion, stream-processing, and historical-analytics consumer groups process the durable stream. ClickHouse stores historical telemetry; the Python ML service derives leakage-safe features and serves maintenance-risk estimates. PostgreSQL remains transactional-only.

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

## Stream processing and anomaly detection

```text
Kafka vehicle.telemetry.v1
          ↓ group: fleetpulse-stream-processor
Schema validation → event-time bounded window state
          ↓
Independent rule detectors
          ↓
Redis event idempotency + alert cooldown
          ↓
Existing PostgreSQL Alert model
```

The processor keeps at most 120 recent events per vehicle in memory and retains them for the configured state TTL. Events are sorted by their original `timestamp`; bounded out-of-order data is admitted to the window, while older data remains validated and counted as late without mutating active state. Redis stores only TTL-scoped event IDs and cooldown keys, not raw telemetry history.

The synthetic rules cover sustained engine overheating, rapid temperature rise, harsh braking derived from consecutive speeds, excessive idling, low battery SOC, low battery SOH, and critical simulator faults. Existing database severity values are used: INFO for idle, WARNING for trend/battery signals, and CRITICAL for overheating/critical faults. These are explainable demo rules, not universal safety classifications.

The alert manager resolves an open alert type after three normal events. Cooldown keys of `alert:{vehicleId}:{alertType}` suppress repeated writes while the condition is sustained. PostgreSQL writes retry three times with short backoff; failure is surfaced for Kafka redelivery. Event IDs are claimed before processing, and claims are released on processing failure. Detection latency is calculated from event timestamp to alert handling time and is reported only when measured.

Sliding-window operations maintain a bounded per-vehicle collection; insertion is sorted and capped, so memory is O(V × W) for V active vehicles and W retained events per vehicle. Detector evaluation scans only the bounded vehicle window, O(W) per event. Cooldown and idempotency lookups are O(1) Redis operations. Horizontal scale is achieved through Kafka partitions and additional stream-processor instances.

## Historical analytics and predictive maintenance

```text
Kafka vehicle.telemetry.v1
          ↓ group: fleetpulse-historical-analytics
ClickHouse MergeTree historical telemetry
          ↓ time-range queries / feature extraction
Chronological ML dataset + synthetic maintenance ground truth
          ↓
Logistic Regression baseline → Random Forest improved model
          ↓
FastAPI maintenance-risk estimate
```

ClickHouse is partitioned by `toDate(event_timestamp)` and ordered by `(vehicle_id, event_timestamp, event_id)`, matching vehicle-history and time-range queries. Local retention is the warm analytical tier; recent data is hot within the same store, while production cold Parquet/object storage is deferred. PostgreSQL continues to own vehicles, fleets, maintenance, alerts, users, and audit records; it does not receive the raw telemetry firehose.

The historical query layer provides telemetry volume by vehicle/day and hour, utilization proxies, temperature summaries, battery SOC/SOH summaries, and fault frequency. The analytical ordering key supplies primary pruning for vehicle/time queries; query latency and EXPLAIN evidence are measured only against a populated local ClickHouse instance and are not fabricated here.

The ML label is `maintenance_required_within_7_days`, generated from a later synthetic `MAINTENANCE_DUE` event. Features use only events at or before the prediction timestamp. Chronological 60/20/20 splits prevent temporally correlated telemetry from leaking across train, validation, and test. Model output is an estimate, never an instruction to control a vehicle.

## Intended future flow

Future AI assistance, frontend views, and production cold storage may consume the historical and prediction boundaries. A controlled AI agent may use authorized fleet tools and curated documentation in a later block.

## Component boundaries

- **API:** transactional and query boundary; currently only `/health`.
- **Kafka:** Phase 3 durable telemetry stream with partitions, replay, and consumer groups.
- **PostgreSQL:** transactional domain data, configuration, users, alerts, and audit records in future phases; raw high-volume telemetry will not primarily live here.
- **Redis:** Phase 3 event-id idempotency state, with future cache, ephemeral vehicle state, coordination, and rate-limiting roles.
- **Telemetry simulator:** Phase 2 software-only vehicle population and event generator. It uses shared state objects rather than one process, worker, or timer per vehicle, and selects Kafka through the existing sink abstraction.
- **Ingestion:** Phase 3 shared Kafka producer boundary and consumer. It performs schema validation, Redis idempotency, controlled processing, and structured metrics/logging; it does not perform analytics or ML.
- **Telemetry contract:** versioned Zod schema in `packages/schemas`, including event identity, event time, delivery time, sequence number, motion, engine, fuel, battery, and fault signals.
- **Analytical storage:** ClickHouse warm historical telemetry with date partitions and vehicle/time ordering; cold object storage is future work.
- **ML service:** Python FastAPI service with reproducible scikit-learn training and inference artifacts.
- **AI agent:** future controlled tool-using fleet assistant with authorization and prompt-injection safeguards.

## Scalability principles

Future designs will use partitioning, idempotent processing, at-least-once delivery, back-pressure, retries, dead-letter handling, bounded APIs, tenant isolation, and measured capacity. Scale targets are claims only after benchmark evidence exists.

## Simulator and future ML data

The simulator deliberately generates correlated state transitions and explainable precursor patterns for engine overheating, battery degradation, brake degradation, and tire pressure issues. `eventId` is the future ingestion idempotency key; `timestamp` is original simulated event time, while `deliveryTimestamp` can differ when delayed delivery is enabled. These events plus future maintenance outcomes provide the raw material for a time-based predictive-maintenance dataset without implementing ML in this phase.

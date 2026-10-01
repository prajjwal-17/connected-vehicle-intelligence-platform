# FleetPulse stream processor

Phase 4 consumes `vehicle.telemetry.v1` in the independent `fleetpulse-stream-processor` consumer group. It validates the shared schema, keeps bounded per-vehicle event-time state, evaluates independently testable rule detectors, and writes only detected alerts to the existing PostgreSQL `Alert` model.

## Rules

Defaults are configurable through `.env`: engine overheating above `110C` sustained across the 30-second window; rapid temperature rise above `0.5C/s`; harsh braking above `25kph/s` deceleration; excessive idling after 300 seconds of ignition-on near-zero speed; low battery below 20% SOC; battery SoH risk below 70%; and critical simulator fault events. Synthetic severities map to the existing database enum: INFO for idle, WARNING for trend/low-battery rules, and CRITICAL for overheating and critical faults. These are explainable demo rules, not universal real-world safety classifications.

## State and event time

State is a bounded sorted sliding window of at most 120 recent events per vehicle, retained for the configured state TTL (15 minutes by default). Event timestamps, not arrival timestamps, drive rates and windows. Events arriving within `MAX_OUT_OF_ORDER_MS` are retained in the bounded window and sorted by event time; older events are still validated and counted as late, but do not mutate the active window or trigger a state transition. No full telemetry history is stored in Redis or PostgreSQL.

Redis stores only `stream:idempotency:{eventId}` and alert cooldown keys, each with TTLs. The same event ID therefore cannot generate repeated processing or alerts. A cooldown key `alert:{vehicleId}:{alertType}` suppresses repeated alerts while an anomaly remains sustained. After three normal events, known open alert types are resolved with the existing `Alert` status lifecycle.

## Reliability and scaling

Kafka offsets are acknowledged only after validation, Redis idempotency, detection, and bounded PostgreSQL alert retry complete. PostgreSQL writes retry three times with short backoff; failures are surfaced to KafkaJS for redelivery rather than silently discarded. Kafka partitions and additional stream-processor instances provide horizontal scaling. Processing is continuous and per-partition; there is no unbounded in-memory queue.

Detection latency is measured as local alert-processing time minus the event timestamp and recorded in structured metrics. No latency or throughput target is claimed without a measured local run.

Run locally after Phase 0 services and database migration/seed are available:

```powershell
docker compose up -d kafka redis postgres
npm run stream-processor
npm run simulator -- --vehicles 10 --duration 10 --rate 10 --output-mode kafka
```

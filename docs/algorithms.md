# FleetPulse algorithms

FleetPulse currently implements synthetic correlated telemetry generation, Kafka application-level idempotency, bounded event-time stream state, anomaly cooldowns, sliding telemetry feature extraction, and chronological ML training/inference. ClickHouse uses date partitioning and an ordering key of `(vehicle_id, event_timestamp, event_id)` for vehicle/time pruning.

The implemented stream rules include overheating, rapid temperature rise, harsh braking, excessive idling, low battery SOC, low battery SOH, and critical faults. The predictive-maintenance model is a seven-day estimate trained from leakage-safe historical features.

Dijkstra, A*, Bloom filters, Count-Min Sketch, geohash indexing, and vector/RAG retrieval are not implemented and are not represented as implemented capabilities.

# Telemetry simulator

The FleetPulse simulator represents connected vehicles entirely in software. It does not use physical vehicles, IoT devices, gateways, or hardware sensors.

## Architecture

```text
Vehicle Population
       ↓
Vehicle State
       ↓
Telemetry Generator
       ↓
Scenario/Fault Engine
       ↓
Schema Validation
       ↓
Bounded Event Sink
```

Vehicles are lightweight in-memory state objects. The engine updates them in a shared loop; it does not create a process, worker, or timer per vehicle. This allows the population to be initialized at 100,000 vehicles while keeping event generation bounded by the configured rate.

## Run

```powershell
npm run simulator -- --vehicles 1000 --duration 10 --rate 100
npm run simulator -- --vehicles 1000 --duration 10 --rate 100 --output ./tmp/events.jsonl
npm run simulator -- --vehicles 100000 --duration 0 --rate 1000
```

The last command is a bounded initialization capability check with zero generated events. When invoked through the root workspace script, relative output paths are resolved from `services/telemetry-simulator/`. JSONL files are local test artifacts and are ignored by Git.

## Configuration

Environment variables include `VEHICLE_COUNT`, `EVENTS_PER_SECOND`, `TIME_ACCELERATION`, `BURST_ENABLED`, `BURST_MULTIPLIER`, `BURST_DURATION_MS`, `DUPLICATE_EVENT_RATE`, `OUT_OF_ORDER_RATE`, `MAX_EVENT_DELAY_MS`, `OUTPUT_MODE`, `OUTPUT_FILE`, `REGION`, `SIMULATION_DURATION_SECONDS`, `SEED`, and `MAX_BUFFER_SIZE`.

CLI flags override the matching development values: `--vehicles`, `--duration`, `--rate`, `--output`, `--burst`, `--duplicates`, `--out-of-order`, and `--seed`.

## Event contract

The versioned Zod contract lives in `packages/schemas/src/telemetry.ts`. Every emitted event contains `schemaVersion`, `eventId`, `vehicleId`, `vin`, `timestamp`, `sequenceNumber`, and telemetry measurements. `eventId` is intended to become the future ingestion idempotency key.

The original event `timestamp` is simulation time. `deliveryTimestamp` is added by an output sink and can differ when out-of-order delivery is enabled. The simulator intentionally emits duplicates and delayed events when configured; Phase 3 ingestion will handle deduplication and ordering.

## Fault scenarios

Explainable precursor signals are available for engine overheating, battery degradation, brake degradation, and tire pressure issues. They evolve from vehicle state and can produce `FAULT` and `MAINTENANCE_DUE` events, providing future maintenance ground truth without implementing ML in this phase.

## Kafka boundary

Only stdout and JSONL sinks are implemented in Phase 2. Kafka producers are intentionally not implemented; Phase 3 will add a Kafka sink without changing the simulation engine or event contract.

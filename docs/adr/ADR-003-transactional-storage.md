# ADR-003: PostgreSQL for transactional data

## Decision

PostgreSQL is the transactional store for FleetPulse domain data.

## Rationale

Relational integrity, transactions, mature tooling, and clear query semantics fit future fleet configuration, alerts, users, permissions, and audit records. Raw high-volume telemetry will not be stored primarily in PostgreSQL because its volume and time-series access patterns require a separately justified analytical or object-storage design.

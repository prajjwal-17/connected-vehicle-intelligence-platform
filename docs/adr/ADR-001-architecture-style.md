# ADR-001: Service boundaries without a distributed monolith

## Decision

FleetPulse uses a modular monorepo with explicit application and service boundaries. We will add a separately deployable service only when its workload, scaling profile, ownership, or reliability needs justify it.

## Rationale

The telemetry path and ML workloads will have different runtime concerns from transactional APIs. Boundaries make those concerns explicit while a monorepo keeps local development and shared contracts manageable. We avoid both a single tangled application and unnecessary microservices that would add operational cost without a clear benefit.

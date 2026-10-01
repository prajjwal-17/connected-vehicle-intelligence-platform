# ADR-007: Product APIs, dashboard, and controlled assistant

## Decision

Block 2 keeps Fastify as the product API boundary. PostgreSQL remains the source of transactional fleet, vehicle, alert, and maintenance data; ClickHouse is queried only for historical telemetry summaries and analytics; the Python service is called for maintenance-risk inference.

The web application is a small Next.js workspace using the existing API. The assistant exposes a fixed registry of product tools and records request ID, selected tool, grounded result, and timestamp in the structured API response/log context. It has no arbitrary SQL, shell, filesystem, or database execution capability. External LLM and vector/RAG integration remain future work and are not required for core product availability.

## Consequences

- Dependency failures are explicit and maintenance risk is never fabricated.
- API and dashboard checks can run against mocked or unavailable analytical/ML services.
- PostgreSQL schema and existing Kafka/stream boundaries remain unchanged.

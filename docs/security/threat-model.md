# FleetPulse STRIDE threat model

| Threat                 | Asset / surface          | Mitigation                                                                                                                          | Residual risk                                                               |
| ---------------------- | ------------------------ | ----------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------- |
| Spoofing               | API identity             | `AUTH_MODE=header` requires an upstream IdP/proxy to provide authenticated identity headers; local mode is disabled for development | External IdP and signed-token validation are deployment requirements        |
| Tampering              | API inputs and telemetry | Zod validation, Prisma parameterization, bounded payloads, Kafka schema validation                                                  | Credentials and TLS must be supplied by deployment                          |
| Repudiation            | Security and AI actions  | Request IDs, structured request logs, AI tool/request audit fields, existing AuditLog model                                         | Durable auth audit persistence is not yet wired for every route             |
| Information disclosure | Tenant data, secrets     | Secrets remain environment/Kubernetes Secret inputs; safe error responses and security headers                                      | Full server-side tenant query scoping requires authenticated tenant context |
| Denial of service      | HTTP/Kafka/analytics     | Request body limit, in-memory per-IP rate limit, readiness endpoint, bounded consumers                                              | Distributed rate limiting and WAF are production requirements               |
| Elevation of privilege | AI tools and roles       | Fixed tool registry; no shell, filesystem, arbitrary SQL, destructive mutation, or alert-closing tool                               | Header mode must be behind a trusted identity proxy with signed claims      |

TLS, encryption at rest, managed secret rotation, complete tenant scoping, and external IdP configuration are production deployment requirements, not claims about this local environment.

# FleetPulse agent instructions

- FleetPulse is a Connected Vehicle Intelligence Platform built around software-simulated vehicles and synthetic telemetry; no physical IoT hardware is required.
- Work in explicit phases. Implement only the requested phase, preserve existing architecture, and stop after that phase.
- Inspect the repository and relevant documentation before modifying files. Do not introduce unnecessary technologies or premature future-phase features.
- Run the relevant tests, typechecks, lint, builds, and smoke checks. Never claim a blocked or unrun check passed.
- Ask for permission before actions requiring elevated access or new permissions. If a process hangs, wait briefly, then stop it safely and report what happened.
- Do not use destructive commands without explicit permission. Do not commit secrets, `.env` files, credentials, or generated local data.
- Treat Docker volumes and database persistence as valuable local state. Do not remove volumes or reset databases unless explicitly requested.
- Use synthetic data only; do not add real vehicle, customer, location, or credential data.
- Keep service boundaries and technology choices justified by the current phase. Avoid unnecessary microservices, dependencies, and infrastructure.

## graphify

This project has a knowledge graph at graphify-out/ with god nodes, community structure, and cross-file relationships.

When the user types `/graphify`, use the installed graphify skill or instructions before doing anything else.

Rules:

- For codebase questions, first run `graphify query "<question>"` when graphify-out/graph.json exists. Use `graphify path "<A>" "<B>"` for relationships and `graphify explain "<concept>"` for focused concepts. These return a scoped subgraph, usually much smaller than GRAPH_REPORT.md or raw grep output.
- Dirty graphify-out/ files are expected after hooks or incremental updates; dirty graph files are not a reason to skip graphify. Only skip graphify if the task is about stale or incorrect graph output, or the user explicitly says not to use it.
- If graphify-out/wiki/index.md exists, use it for broad navigation instead of raw source browsing.
- Read graphify-out/GRAPH_REPORT.md only for broad architecture review or when query/path/explain do not surface enough context.
- After modifying code, run `graphify update .` to keep the graph current (AST-only, no API cost).

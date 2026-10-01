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

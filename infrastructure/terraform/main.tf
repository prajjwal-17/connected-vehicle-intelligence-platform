terraform {
  # Provider-specific resources are intentionally left to the deployment target.
  # Expected managed services: PostgreSQL, Redis, Kafka, ClickHouse/object storage,
  # container platform, secret manager, and monitoring.
}

output "environment" { value = var.environment }
output "region" { value = var.region }
output "container_platform" { value = var.container_platform }

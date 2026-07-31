# ADR-012 — Manage a Stable WebSocket Custom Domain in the Dev Workspace

- Status: Accepted

## Decision

Create ACM, Route 53 validation, API Gateway Regional custom domain, root API mapping and alias in the same Terraform workspace as the API.

## Consequences

Positive: stable client URL across API recreation.

Negative: destroy/apply causes temporary endpoint downtime and recreates certificate/domain resources.

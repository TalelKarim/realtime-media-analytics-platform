# ADR-001 — Use ECS Fargate for the Wikimedia Collector

- Status: Accepted
- Date: 2026

## Context

The source is a long-lived Server-Sent Events connection. The process must remain connected, reconnect, buffer records and flush on shutdown.

## Decision

Run one Python Collector as an ECS Fargate service with an Alloy sidecar.

## Consequences

Positive: natural long-running runtime, health/restart management, no Lambda duration limit, sidecar telemetry.

Negative: permanent cost, NAT/network operations, one-task restart gap and possible in-memory-buffer loss.

## Alternatives

Lambda was rejected for the persistent SSE lifecycle. EC2/EKS would add unnecessary host/platform management for the current scale.

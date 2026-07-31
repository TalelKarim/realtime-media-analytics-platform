# ADR-006 — Use API Gateway WebSocket for the Live Dashboard

- Status: Accepted

## Decision

Use API Gateway WebSocket routes for lifecycle and Management API `postToConnection` for server push.

## Consequences

Positive: managed connections, no socket server fleet, native Lambda integration and stable custom domain.

Negative: one Management API call per connection/chunk, account quotas, no native multicast and a maximum connection duration/idle behavior that requires heartbeat/reconnect.

## Delivery model

Best-effort latest state, not guaranteed delivery of every snapshot.

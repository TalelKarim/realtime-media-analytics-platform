# ADR-011 — Measure Backend Push Freshness from Wikimedia Event Time

- Status: Accepted with follow-up required

## Decision

Record successful `postToConnection` time minus the least-fresh latest source timestamp in each chunk.

## Consequences

Positive: includes nearly the full backend and measures fan-out position per client.

Negative: successful chunks only, approximate histogram, timestamp/state not atomic and no browser receipt/render proof.

## Follow-up

Add k6 receive-time and browser DOM-time ground truth before calling it user-visible freshness.

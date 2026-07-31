# Security

## 1. Security model

The platform uses managed AWS identity, encryption and network controls. Security responsibilities are split by component rather than using one shared execution role.

## 2. IAM separation

Distinct roles exist for:

```text
ECS Collector execution
ECS Collector task
Realtime Processor
Alert Processor
Broadcast Coordinator
Broadcast Worker
WebSocket connect/default/disconnect
Firehose
Glue
GitHub dashboard deployment
Grafana AWS integration
```

Each role should contain only the stream, queue, table, topic or API actions required by that component.

## 3. Encryption at rest

Customer-managed KMS keys protect:

```text
Kinesis
DynamoDB
SQS
S3 Data Lake
CloudWatch Logs
```

Firehose and Glue roles require both resource access and the corresponding KMS actions.

## 4. Encryption in transit

```text
Wikimedia SSE         HTTPS
AWS service calls     TLS and SigV4
WebSocket client      WSS
Worker Management API HTTPS and IAM
OTLP external export  HTTPS
```

## 5. Secrets

Collector Grafana OTLP authorization is stored in Secrets Manager and injected through the ECS execution path. Lambda Grafana authorization is currently supplied to Terraform as a sensitive variable and passed as an environment variable to the local OTel extension.

Do not commit real authorization headers or Terraform variable files containing them.

## 6. Network

The Collector runs in the VPC and reaches the public Wikimedia endpoint through NAT. VPC endpoints reduce public routing for supported AWS service calls.

API Gateway WebSocket is a public Regional endpoint protected by TLS and API Gateway service controls. The project does not currently implement end-user authentication/authorization on `$connect`; it is a public demo endpoint.

## 7. WebSocket input validation

The default handler accepts only:

```text
subscribe
unsubscribe
```

Topics are restricted to:

```text
global
top_pages
wiki:[a-z0-9_-]{2,80}
```

A connection can hold at most 50 topics and `global` cannot be removed.

## 8. Abuse and production hardening

For public production use, add:

- authentication on `$connect`;
- authorization for allowed topics;
- WAF/rate protection where supported;
- per-user connection and subscription limits;
- origin/application checks;
- message size validation at the edge;
- audit events for privileged subscription changes;
- cost anomaly alerts.

## 9. Data classification

Wikimedia recentchange is public data, but fields can contain public usernames, comments and page titles. Treat the archive as public-source data that still requires controlled access, retention policy and responsible use.

## 10. Deletion and recovery

DynamoDB point-in-time recovery and deletion protection are configurable but disabled by default in the dev module. The dev environment is intentionally destroyable. A production environment should enable protection and define backup/restore tests.

## 11. Supply chain

- pin Terraform provider versions and commit lock files;
- use immutable/versioned container tags for the Collector;
- build Lambda dependencies for the target Linux architecture;
- scan Python/npm dependencies and container images;
- protect branch `v2`/release branches;
- use GitHub OIDC rather than long-lived AWS keys.

## 12. Known security gaps

- public unauthenticated WebSocket connection;
- no tenant model;
- broad demo observability access is external to AWS;
- legacy V1 IAM role and transitional table permissions remain until cleanup;
- sensitive Terraform input handling depends on Terraform Cloud workspace controls.

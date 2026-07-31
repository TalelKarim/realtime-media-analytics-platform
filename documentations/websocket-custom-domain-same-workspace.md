# WebSocket custom domain in the same dev workspace

Fixed public endpoint:

```text
wss://stream-websocket.talelkarimchebbi.com
```

The normal dev Terraform workspace owns all resources:

```text
ACM certificate
Route53 DNS validation record
API Gateway v2 Regional custom domain
API mapping to the current WebSocket API/stage
Route53 A alias
```

A full destroy removes those resources and the WebSocket API. The next apply recreates them with a new API ID but the exact same public FQDN. The frontend URL therefore never needs to change.

There is expected downtime between destroy and the end of the next apply.

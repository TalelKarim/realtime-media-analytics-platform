# WebSocket Custom Domain — Same Terraform Workspace

## Stable public endpoint

```text
wss://stream-websocket.talelkarimchebbi.com
```

The `dev` Terraform workspace owns the complete chain:

```text
ACM certificate
Route 53 DNS validation record
API Gateway v2 Regional custom domain
API mapping to the current WebSocket API and dev stage
Route 53 A alias
```

## Why it exists

The generated `execute-api` identifier changes after a full destroy/apply. The custom hostname remains stable, so the frontend and load-test scripts do not require configuration changes after recreation.

## Root mapping

The API mapping key is empty. Clients connect to:

```text
wss://stream-websocket.talelkarimchebbi.com
```

They do not append `/dev`; the stage mapping is internal.

## Destroy/apply behavior

A full destroy removes:

- the WebSocket API;
- the Regional custom domain;
- the ACM certificate and validation records;
- the API mapping;
- the Route 53 alias.

The next apply recreates them with the same FQDN but a new API ID. Downtime is expected between destroy and the completion of apply and DNS/certificate readiness.

## Management endpoint

Workers use the API Gateway-generated management endpoint from Terraform, not the public WSS URL. `postToConnection` is an HTTPS Management API call executed with IAM authorization.

## Corporate network caveat

The custom-domain WebSocket was validated successfully from a personal mobile network and AWS CloudShell. Failures observed only through the corporate browser/network were caused by corporate CSP/proxy/security controls, not by API Gateway or Terraform.

Validation must therefore include at least one independent network path.

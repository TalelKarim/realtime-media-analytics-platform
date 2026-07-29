# Broadcasting V2 shard-centric — Phase 1

## Objectif

Cette phase installe les fondations du nouveau fan-out sans activer le nouveau Worker.

Elle réalise :

1. un GSI `connection-shard-index` sur `websocket_connections` ;
2. le dual-write de `connection_shard` dans les handlers WebSocket ;
3. les timestamps de fraîcheur par topic et par fenêtre dans le signal du Realtime Processor ;
4. les snapshots V3, manifests, pointeur `LATEST` et verrou d'idempotence dans le Coordinator ;
5. la préparation de `N` jobs shard-centric par manifest, où `N = CONNECTION_SHARD_COUNT` ;
6. la désactivation volontaire de la publication des jobs et du mapping Worker pendant la Phase 1.

Le Worker et le frontend restent inchangés dans cette phase. Le dashboard ne reçoit donc pas encore le nouveau format V3.

## Contrat des données

### `websocket_connections`

```json
{
  "connection_id": "abc123",
  "connection_shard": "SHARD#01",
  "subscription_shard": 1,
  "topics": ["global", "wiki:frwiki"],
  "ttl": 1785340000
}
```

Le champ `subscription_shard` et la table `websocket_subscriptions` sont conservés temporairement pour le dual-write.

### Signal Realtime Processor V3

```json
{
  "schema_version": 3,
  "message_type": "aggregates.updated",
  "signal_id": "BROADCAST#...",
  "sequence": 1785330000000,
  "updated_topics": ["global", "top_pages", "wiki:frwiki"],
  "event_timestamp_bounds_by_topic_by_window": {
    "2026-07-29T14:00:00Z": {
      "global": {
        "oldest_event_timestamp_ms": 1785330000100,
        "latest_event_timestamp_ms": 1785330000900
      },
      "wiki:frwiki": {
        "oldest_event_timestamp_ms": 1785330000400,
        "latest_event_timestamp_ms": 1785330000900
      }
    }
  }
}
```

### Table `broadcast_snapshots`

La table contient quatre types d'items :

- `SNAPSHOT` : données réelles d'un topic ;
- `MANIFEST` : références de tous les snapshots d'une fenêtre ;
- `POINTER` : item `LATEST/MANIFEST` ;
- `IDEMPOTENCY` : état d'un signal déjà traité.

## Garde-fou de Phase 1

```hcl
broadcast_shard_jobs_enabled = false
```

Cette valeur est le défaut Terraform. Elle entraîne :

```text
Coordinator : PUBLISH_SHARD_JOBS=false
Worker event source mapping : disabled
```

Le Coordinator crée les snapshots et les manifests, mais n'alimente pas `broadcast-jobs.fifo`.

## Validation locale

```bash
./scripts/validate_phase1_shard_centric.sh
```

## Déploiement Terraform Cloud

```bash
git add \
  services/realtime-processor/src/handler.py \
  services/broadcast-coordinator/src/handler.py \
  services/websocket-connect-handler/src/handler.py \
  services/websocket-default-handler/src/handler.py \
  services/websocket-disconnect-handler/src/handler.py \
  .build/lambdas/realtime-processor/src \
  .build/lambdas/broadcast-coordinator/src \
  .build/lambdas/websocket-connect-handler/src \
  .build/lambdas/websocket-default-handler/src \
  .build/lambdas/websocket-disconnect-handler/src \
  terraform/modules/dynamodb/main.tf \
  terraform/modules/dynamodb/outputs.tf \
  terraform/modules/iam/main.tf \
  terraform/environments/dev/lambda_broadcasting_enhanced.tf \
  terraform/environments/dev/lambda_websocket_handlers.tf \
  terraform/environments/dev/braodcasting_enhanced_event_sources.tf \
  terraform/environments/dev/variables.tf \
  scripts/validate_phase1_shard_centric.sh \
  documentation/phase1-shard-centric-migration.md

git commit -m "feat(broadcasting-v2): add shard-centric phase 1 foundations"
git push origin v2
```

Le plan Terraform Cloud attendu doit modifier principalement :

- la table `websocket_connections` pour ajouter le GSI ;
- les Lambdas Realtime Processor, Coordinator et handlers WebSocket ;
- la policy IAM du Coordinator ;
- le mapping SQS vers le Worker, qui devient désactivé.

## Vérifications AWS après l'Apply

### 1. Purger les anciens jobs V2

```bash
export AWS_REGION="us-east-1"

export JOBS_QUEUE_URL="$(
  aws sqs get-queue-url \
    --region "$AWS_REGION" \
    --queue-name "realtime-media-analytics-dev-broadcast-jobs.fifo" \
    --query QueueUrl \
    --output text
)"

aws sqs purge-queue \
  --region "$AWS_REGION" \
  --queue-url "$JOBS_QUEUE_URL"
```

### 2. Vérifier le GSI

```bash
aws dynamodb describe-table \
  --region "$AWS_REGION" \
  --table-name "realtime-media-analytics-dev-websocket-connections" \
  --query 'Table.GlobalSecondaryIndexes[?IndexName==`connection-shard-index`].{IndexName:IndexName,IndexStatus:IndexStatus,Backfilling:Backfilling}'
```

Résultat attendu : `IndexStatus = ACTIVE`.

### 3. Reconnecter les clients

Les anciennes connexions ne possèdent pas `connection_shard`. Fermer et rouvrir les connexions WebSocket après l'Apply.

### 4. Vérifier les connexions et le GSI

```bash
aws dynamodb scan \
  --region "$AWS_REGION" \
  --table-name "realtime-media-analytics-dev-websocket-connections" \
  --projection-expression 'connection_id, connection_shard, subscription_shard, topics'
```

Puis interroger un shard réellement observé, par exemple :

```bash
aws dynamodb query \
  --region "$AWS_REGION" \
  --table-name "realtime-media-analytics-dev-websocket-connections" \
  --index-name "connection-shard-index" \
  --key-condition-expression 'connection_shard = :shard' \
  --expression-attribute-values '{":shard":{"S":"SHARD#01"}}' \
  --projection-expression 'connection_id, connection_shard, topics'
```

### 5. Vérifier le signal V3

```bash
aws logs tail \
  "/aws/lambda/realtime-media-analytics-dev-realtime-processor" \
  --region "$AWS_REGION" \
  --since 10m \
| grep -E 'broadcast_signal_sent|event_timestamp_bounds_by_topic_by_window'
```

### 6. Vérifier les manifests et snapshots

```bash
aws dynamodb scan \
  --region "$AWS_REGION" \
  --table-name "realtime-media-analytics-dev-broadcast-snapshots" \
  --filter-expression 'item_type = :manifest' \
  --expression-attribute-values '{":manifest":{"S":"MANIFEST"}}' \
  --projection-expression 'snapshot_id, sequence, aggregation_window, updated_topics'
```

```bash
aws dynamodb get-item \
  --region "$AWS_REGION" \
  --table-name "realtime-media-analytics-dev-broadcast-snapshots" \
  --key '{"snapshot_id":{"S":"LATEST"},"topic":{"S":"MANIFEST"}}' \
  --consistent-read
```

### 7. Vérifier l'idempotence

```bash
aws dynamodb scan \
  --region "$AWS_REGION" \
  --table-name "realtime-media-analytics-dev-broadcast-snapshots" \
  --filter-expression 'item_type = :kind' \
  --expression-attribute-values '{":kind":{"S":"IDEMPOTENCY"}}' \
  --projection-expression 'snapshot_id, #status, sequence, jobs_planned, jobs_published' \
  --expression-attribute-names '{"#status":"status"}'
```

Résultat attendu :

```text
status         = COMPLETED
jobs_planned   = nombre_de_fenêtres × nombre_de_shards
jobs_published = 0
```

### 8. Vérifier que la queue reste vide

```bash
aws sqs get-queue-attributes \
  --region "$AWS_REGION" \
  --queue-url "$JOBS_QUEUE_URL" \
  --attribute-names \
    ApproximateNumberOfMessages \
    ApproximateNumberOfMessagesNotVisible
```

Les deux valeurs doivent rester à `0` pendant la Phase 1.

### 9. Vérifier que le Worker est désactivé

```bash
aws lambda list-event-source-mappings \
  --region "$AWS_REGION" \
  --function-name "realtime-media-analytics-dev-broadcast-worker" \
  --query 'EventSourceMappings[].{State:State,UUID:UUID,LastProcessingResult:LastProcessingResult}'
```

Le mapping `broadcast-jobs.fifo` doit être `Disabled`.

## Critère de validation de Phase 1

Pour un signal avec une fenêtre et trois shards :

```text
snapshots_created = nombre de topics modifiés
manifests_created = 1
jobs_planned      = 3
jobs_published    = 0
queue jobs        = 0
```

La Phase 2 remplacera le Worker, ajoutera le Query du GSI, le BatchGet des snapshots, le batching/chunking, les deux checks `LATEST`, le nouveau format frontend et activera `broadcast_shard_jobs_enabled = true`.

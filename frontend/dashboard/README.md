# Realtime Media Analytics Dashboard

Frontend React/TypeScript pour le projet **Realtime Media Analytics Platform on AWS**.

Ce dashboard visualise en live les agrégats poussés par ton backend WebSocket :

```text
React Dashboard
→ API Gateway WebSocket WSS
→ WebSocket Lambdas
→ DynamoDB connections / subscriptions
→ Broadcaster Lambda
→ stats.update
→ UI live
```

Stack :

```text
React 18
TypeScript
Vite
Tailwind CSS
Recharts
WebSocket natif du navigateur
```

---

## 1. Prérequis

Installe Node.js 20+.

Vérifie :

```bash
node -v
npm -v
```

Il faut aussi avoir ton WebSocket API Gateway déjà déployé, par exemple :

```text
wss://xxxx.execute-api.us-east-1.amazonaws.com/dev
```

---

## 2. Installation locale

Dézippe le projet puis entre dans le dossier :

```bash
cd realtime-media-analytics-dashboard
```

Installe les dépendances :

```bash
npm install
```

Copie le fichier d'environnement :

```bash
cp .env.example .env
```

Édite `.env` :

```bash
VITE_WS_URL=wss://xxxx.execute-api.us-east-1.amazonaws.com/dev
VITE_DEFAULT_TOPICS=global
VITE_HEARTBEAT_ACTION=heartbeat
VITE_HEARTBEAT_INTERVAL_MS=240000
VITE_ENABLE_DEMO_DATA=false
```

Lance en local :

```bash
npm run dev
```

Ouvre :

```text
http://localhost:5173
```

---

## 3. Récupérer l'URL WebSocket depuis Terraform

Selon tes outputs Terraform, tu peux avoir un output du style :

```bash
terraform output
```

Cherche une valeur comme :

```text
wss://xxxx.execute-api.us-east-1.amazonaws.com/dev
```

Puis mets-la dans `.env` :

```bash
VITE_WS_URL=wss://xxxx.execute-api.us-east-1.amazonaws.com/dev
```

Important : les variables `VITE_*` sont injectées au build Vite. Si tu changes `.env`, redémarre le serveur local.

---

## 4. Contrat WebSocket attendu

Le frontend envoie des messages JSON simples.

### Subscribe

```json
{
  "action": "subscribe",
  "topic": "global"
}
```

### Unsubscribe

```json
{
  "action": "unsubscribe",
  "topic": "global"
}
```

### Heartbeat

```json
{
  "action": "heartbeat",
  "client_type": "dashboard",
  "sent_at": "2026-07-03T13:10:00.000Z"
}
```

Le frontend ne met jamais à jour DynamoDB directement. Le heartbeat doit être traité côté `$default` Lambda ou route dédiée, et le backend doit rafraîchir le TTL de la connexion dans DynamoDB.

---

## 5. Format de message reçu attendu

Le broadcaster doit pousser des messages du type :

```json
{
  "type": "stats.update",
  "topic": "global",
  "data": {
    "event_count": 1248,
    "bot_count": 421,
    "human_count": 827,
    "top_wikis": [
      { "wiki": "commonswiki", "count": 426 },
      { "wiki": "wikidatawiki", "count": 258 }
    ],
    "change_types": [
      { "change_type": "edit", "count": 636 },
      { "change_type": "categorize", "count": 412 }
    ],
    "namespace_distribution": [
      { "namespace": 14, "count": 455 },
      { "namespace": 0, "count": 317 }
    ],
    "top_pages": [
      {
        "wiki": "commonswiki",
        "namespace": 14,
        "title": "Category:Images from Wikimedia Commons",
        "count": 18,
        "bot_count": 6,
        "human_count": 12
      }
    ]
  }
}
```

Le normalizer du frontend est volontairement tolérant : il accepte aussi des variantes comme `topWikis`, `eventCount`, `events_count`, `change_type_distribution`, etc.

---

## 6. Fonctionnalités incluses

Le dashboard inclut :

```text
- connexion WSS native
- reconnexion automatique avec backoff exponentiel + jitter
- heartbeat configurable toutes les 4 minutes par défaut
- resubscribe automatique après reconnexion
- subscribe/unsubscribe depuis l'UI
- état connected / reconnecting / disconnected
- cards KPI live
- top wikis chart
- bot vs human donut
- change type donut
- namespace chart
- top pages table
- event log technique
- UI responsive et dark mode
```

---

## 7. Pourquoi le heartbeat est important

API Gateway WebSocket coupe les connexions idle après un certain temps et impose aussi une durée maximale de connexion. Le frontend envoie donc un heartbeat toutes les 4 minutes par défaut pour :

```text
1. éviter une connexion idle
2. permettre au backend de rafraîchir le TTL DynamoDB
3. détecter plus vite les connexions mortes
```

Si ton backend ne supporte pas encore `action=heartbeat`, ajoute un case dans la Lambda `$default` pour mettre à jour le TTL de la connexion et répondre éventuellement avec :

```json
{ "type": "heartbeat.ack" }
```

---

## 8. Build production

```bash
npm run build
```

Le build sort dans :

```text
dist/
```

Prévisualiser le build :

```bash
npm run preview
```

---

## 9. Déploiement S3 + CloudFront

Exemple manuel en dev :

```bash
npm run build
aws s3 sync dist/ s3://<frontend-bucket>/ --delete
aws cloudfront create-invalidation --distribution-id <distribution-id> --paths "/*"
```

Pour une version propre, crée un module Terraform frontend avec :

```text
S3 bucket privé
CloudFront distribution
Origin Access Control
ACM certificate optionnel
Route53 record optionnel
```

---

## 10. Troubleshooting

### Le dashboard reste vide

Vérifie que le WSS URL est correct dans `.env`, puis regarde le panneau `Realtime events`.

Teste aussi la connexion WebSocket avec Hoppscotch :

```json
{ "action": "subscribe", "topic": "global" }
```

### Connexion puis déconnexion immédiate

Vérifie les logs API Gateway WebSocket et la Lambda `$connect`.

### `heartbeat` retourne une erreur backend

Ton `$default` Lambda ne supporte probablement pas encore l'action `heartbeat`. Ajoute ce support côté backend ou change `VITE_HEARTBEAT_ACTION` vers l'action supportée.

### Aucun update reçu

Vérifie :

```text
Broadcaster Lambda logs
DynamoDB websocket_connections
topics stockés sur la connexion
SQS broadcast signal
DynamoDB aggregates
```

### Plusieurs updates dupliqués

Vérifie que le backend ne garde pas des connexions fantômes. Le frontend évite les listeners dupliqués et resubscribe proprement après reconnect, mais le backend doit supprimer les connexions GoneException.

---

## 11. Structure du projet

```text
src/
├── App.tsx
├── config.ts
├── hooks/
│   └── useRealtimeWebSocket.ts
├── lib/
│   ├── normalize.ts
│   ├── format.ts
│   └── demoData.ts
├── components/
│   ├── ConnectionStatus.tsx
│   ├── SettingsPanel.tsx
│   ├── TopicSelector.tsx
│   ├── MetricCard.tsx
│   ├── ChartCard.tsx
│   ├── EventLog.tsx
│   └── charts/
│       ├── TopWikisChart.tsx
│       ├── BotHumanDonut.tsx
│       ├── ChangeTypeChart.tsx
│       ├── NamespaceChart.tsx
│       └── TopPagesTable.tsx
└── index.css
```

---

## 12. Commandes utiles

```bash
npm install
npm run dev
npm run build
npm run preview
npm run lint
```

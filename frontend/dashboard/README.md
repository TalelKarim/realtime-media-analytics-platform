# Realtime Media Analytics Dashboard

Frontend React/TypeScript pour le projet **Realtime Media Analytics Platform on AWS**.

Le dashboard visualise en temps réel les agrégats poussés par le backend WebSocket :

```text
React Dashboard
→ API Gateway WebSocket WSS
→ WebSocket Lambdas
→ DynamoDB connections / subscriptions
→ Broadcaster Lambda
→ stats.update
→ UI live
```

Stack utilisée :

```text
React 18
TypeScript strict
Vite
Tailwind CSS
Recharts
WebSocket natif du navigateur
```

---

## 1. Ce qui est inclus

```text
- UI dark premium responsive
- connexion WSS native
- reconnexion automatique avec backoff exponentiel + jitter
- heartbeat configurable, 4 minutes par défaut
- resubscribe automatique après reconnexion
- subscribe/unsubscribe depuis l'UI
- settings persistés dans localStorage
- protection contre les URLs WSS mal formatées avec guillemets/slash final
- cards KPI live
- top wikis chart
- bot vs human donut
- change type donut
- namespace chart
- top pages table
- event log technique
- build TypeScript strict validé
```

Le dashboard est conçu pour afficher la **fenêtre live courante**. Si ton backend agrège par minute, les compteurs peuvent monter pendant la minute puis repartir à zéro à la fenêtre suivante. C'est normal pour une vue temps réel.

---

## 2. Prérequis

Node.js 20+ est requis.

Vérifie :

```bash
node -v
npm -v
```

Il faut aussi avoir ton API Gateway WebSocket déjà déployée, par exemple :

```text
wss://xxxx.execute-api.us-east-1.amazonaws.com/dev
```

---

## 3. Installation locale

Depuis le dossier du frontend :

```bash
cd frontend/dashboard
npm install
```

Si tu pars d'un zip standalone :

```bash
cd dashboard
npm install
```

---

## 4. Configuration locale

Le zip contient déjà un fichier `.env` si tu l'as fourni. Sinon, crée-le à partir du template :

```bash
cp .env.example .env
```

Exemple de `.env` :

```env
VITE_WS_URL=wss://xxxx.execute-api.us-east-1.amazonaws.com/dev
VITE_DEFAULT_TOPICS=global
VITE_HEARTBEAT_ACTION=heartbeat
VITE_HEARTBEAT_INTERVAL_MS=240000
VITE_ENABLE_DEMO_DATA=false
```

Notes importantes :

```text
- évite les guillemets autour de VITE_WS_URL
- évite le slash final si possible
- si tu modifies .env, redémarre npm run dev
```

Le code nettoie quand même les guillemets et le slash final pour éviter l'erreur navigateur du type :

```text
Failed to construct 'WebSocket': The URL '"wss:' is invalid
```

---

## 5. Lancer l'application en local

```bash
npm run dev
```

Puis ouvre :

```text
http://localhost:5173
```

Si le dashboard reste vide mais indique `Connected`, ouvre Chrome DevTools → Network → WebSocket → Frames et vérifie les messages reçus.

---

## 6. Build production

```bash
npm run build
```

Le build doit passer avec :

```text
tsc -b && vite build
```

Le résultat est généré dans :

```text
dist/
```

Prévisualiser le build :

```bash
npm run preview
```

Puis ouvre :

```text
http://localhost:4173
```

---

## 7. Contrat WebSocket côté frontend

### Subscribe

Le frontend envoie :

```json
{
  "action": "subscribe",
  "topic": "global"
}
```

Exemples de topics :

```text
global
top_pages
wiki:frwiki
wiki:enwiki
wiki:commonswiki
wiki:wikidatawiki
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
  "sent_at": "2026-07-03T13:10:00.000Z",
  "topics": ["global", "wiki:frwiki"]
}
```

Le frontend ne met jamais à jour DynamoDB directement. Le heartbeat doit être traité côté Lambda WebSocket (`$default` ou route dédiée), qui rafraîchit le TTL de la connexion dans DynamoDB.

---

## 8. Message `stats.update` attendu

Format recommandé côté Broadcaster :

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

Le normalizer du frontend est volontairement tolérant. Il accepte aussi des variantes comme :

```text
eventCount / events_count
topWikis / top_wikis
changeTypes / change_type_distribution
namespaceDistribution / namespace_distribution
topPages / top_pages
```

---

## 9. Comportement attendu par topic

### Topic `global`

```text
- Events in current window
- Bot events
- Human events
- Top wikis
- Bot vs human
- Change type distribution
- Namespace activity
- Top pages globales
```

### Topic `wiki:<wiki>`

Exemple :

```text
wiki:commonswiki
```

Comportement idéal côté backend :

```text
- event_count du wiki
- bot_count / human_count du wiki
- change_types du wiki
- namespace_distribution du wiki
- top_pages du wiki
```

Le frontend masque volontairement le chart `Top wikis` pour les topics `wiki:*`, car la vue est déjà filtrée sur un seul wiki.

Si seuls les events s'affichent pour `wiki:commonswiki`, le frontend n'est probablement pas cassé : cela veut dire que le Broadcaster n'envoie pas encore les métriques détaillées pour les topics `wiki:*`.

---

## 10. GitHub Actions — build & deploy S3 + CloudFront

Exemple de workflow à placer à la racine du repo :

```yaml
name: Deploy Realtime Dashboard

on:
  push:
    branches:
      - main
    paths:
      - "frontend/dashboard/**"
      - ".github/workflows/deploy-dashboard.yml"

permissions:
  id-token: write
  contents: read

env:
  AWS_REGION: eu-west-1
  APP_DIR: frontend/dashboard

jobs:
  deploy:
    name: Build and deploy Realtime Dashboard
    runs-on: ubuntu-latest

    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Setup Node.js
        uses: actions/setup-node@v4
        with:
          node-version: 20
          cache: npm
          cache-dependency-path: frontend/dashboard/package-lock.json

      - name: Install dependencies
        working-directory: ${{ env.APP_DIR }}
        run: npm ci

      - name: Build frontend
        working-directory: ${{ env.APP_DIR }}
        env:
          VITE_WS_URL: ${{ secrets.VITE_WS_URL }}
          VITE_DEFAULT_TOPICS: global
          VITE_HEARTBEAT_ACTION: heartbeat
          VITE_HEARTBEAT_INTERVAL_MS: 240000
          VITE_ENABLE_DEMO_DATA: false
        run: npm run build

      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: ${{ secrets.AWS_DEPLOY_ROLE_ARN }}
          aws-region: ${{ env.AWS_REGION }}

      - name: Sync build to S3
        run: |
          aws s3 sync "${APP_DIR}/dist/" "s3://${{ secrets.DASHBOARD_BUCKET_NAME }}" --delete

      - name: Invalidate CloudFront cache
        run: |
          aws cloudfront create-invalidation \
            --distribution-id "${{ secrets.CLOUDFRONT_DISTRIBUTION_ID }}" \
            --paths "/*"
```

Secrets GitHub nécessaires :

```text
AWS_DEPLOY_ROLE_ARN
DASHBOARD_BUCKET_NAME
CLOUDFRONT_DISTRIBUTION_ID
VITE_WS_URL
```

---

## 11. Déploiement manuel S3 + CloudFront

```bash
npm run build
aws s3 sync dist/ s3://<dashboard-bucket-name> --delete
aws cloudfront create-invalidation --distribution-id <distribution-id> --paths "/*"
```

---

## 12. Dépannage rapide

### `zsh: command not found: npm`

Node.js/npm n'est pas installé sur la machine.

Sur macOS avec Homebrew :

```bash
brew install node@20
echo 'export PATH="$(brew --prefix node@20)/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
node -v
npm -v
```

### `Could not find declaration file for module react`

Les types React ne sont pas installés ou pas dans le lockfile.

```bash
npm install --save-dev @types/react@18 @types/react-dom@18
npm run build
```

### L'app se connecte, mais les charts sont vides

Vérifie dans DevTools → Network → WebSocket → Frames que le backend envoie bien un message `stats.update` contenant les champs attendus.

### Le topic `wiki:commonswiki` n'affiche que le compteur

C'est cohérent si le backend n'envoie que `event_count` pour les topics `wiki:*`. Il faut enrichir le Broadcaster pour envoyer aussi `bot_count`, `human_count`, `change_types`, `namespace_distribution` et `top_pages` pour chaque wiki.

---

## 13. Qualité validée

Le code a été corrigé pour passer :

```bash
npm install
npm run build
```

Build validé avec TypeScript strict et Vite production.

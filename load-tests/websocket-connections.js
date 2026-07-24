import { WebSocket } from 'k6/websockets';
import { sleep } from 'k6';
import { Counter, Gauge, Rate } from 'k6/metrics';

const CONNECTIONS = Number(__ENV.CONNECTIONS || 10);
const HOLD_SECONDS = Number(__ENV.HOLD_SECONDS || 600);
const RAMP_SECONDS = Number(__ENV.RAMP_SECONDS || 30);

const connectionsOpened = new Counter('ws_connections_opened');
const connectionsClosed = new Counter('ws_connections_closed');
const messagesReceived = new Counter('ws_messages_received');
const connectionErrors = new Counter('ws_connection_errors');
const unexpectedDisconnects = new Counter('ws_unexpected_disconnects');
const activeConnections = new Gauge('ws_active_connections');
const connectionSuccessRate = new Rate('ws_connection_success_rate');

export const options = {
  scenarios: {
    websocket_connections: {
      executor: 'per-vu-iterations',
      vus: CONNECTIONS,
      iterations: 1,
      maxDuration: `${HOLD_SECONDS + RAMP_SECONDS + 60}s`,
    },
  },

  thresholds: {
    ws_connection_success_rate: ['rate>0.99'],
    ws_connection_errors: ['count==0'],
  },
};

export default function () {
  /*
   * Étale les ouvertures de connexions sur RAMP_SECONDS.
   *
   * Exemple :
   * 100 connexions, ramp de 20 secondes
   * → environ 5 nouvelles connexions par seconde.
   */
  const staggerSeconds =
    CONNECTIONS > 1
      ? ((__VU - 1) / (CONNECTIONS - 1)) * RAMP_SECONDS
      : 0;

  sleep(staggerSeconds);

  const wsUrl = __ENV.WS_URL;

  if (!wsUrl) {
    throw new Error('WS_URL is required');
  }

  /*
   * Cette valeur doit être exactement le message envoyé
   * par ton frontend lorsqu’il souscrit au topic global.
   */
  const subscribePayload =
    __ENV.SUBSCRIBE_PAYLOAD ||
    JSON.stringify({
      action: 'subscribe',
      topic: 'global',
    });

  const ws = new WebSocket(wsUrl);

  let opened = false;
  let expectedClose = false;
  let heartbeatTimer;
  let closeTimer;

  ws.addEventListener('open', () => {
    opened = true;

    connectionsOpened.add(1);
    activeConnections.add(1);
    connectionSuccessRate.add(true);

    ws.send(subscribePayload);

    /*
     * Ping réseau réel pour maintenir la connexion active.
     * À ajuster selon le heartbeat déjà utilisé par ton frontend.
     */
    heartbeatTimer = setInterval(() => {
      try {
        ws.ping();
      } catch (error) {
        connectionErrors.add(1);
      }
    }, 30000);

    /*
     * La connexion reste réellement ouverte pendant HOLD_SECONDS.
     */
    closeTimer = setTimeout(() => {
      expectedClose = true;

      clearInterval(heartbeatTimer);
      ws.close();
    }, HOLD_SECONDS * 1000);
  });

  ws.addEventListener('message', (event) => {
    messagesReceived.add(1);

    /*
     * Plus tard, on ajoutera ici :
     * - freshness côté client
     * - broadcast_window
     * - messages hors ordre
     * - doublons
     */
    try {
      JSON.parse(event.data);
    } catch (_) {
      // Certains messages peuvent ne pas être du JSON.
    }
  });

  ws.addEventListener('error', () => {
    connectionErrors.add(1);

    if (!opened) {
      connectionSuccessRate.add(false);
    }
  });

  ws.addEventListener('close', () => {
    clearInterval(heartbeatTimer);
    clearTimeout(closeTimer);

    connectionsClosed.add(1);

    if (opened) {
      activeConnections.add(-1);
    }

    if (!expectedClose) {
      unexpectedDisconnects.add(1);
    }
  });
}
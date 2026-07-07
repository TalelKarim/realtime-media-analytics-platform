import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { parseJsonMessage } from '../lib/normalize';
import type { ConnectionStatus, EventLogEntry, RawRealtimeMessage } from '../types/realtime';

interface UseRealtimeWebSocketOptions {
  url: string;
  enabled: boolean;
  defaultTopics: string[];
  heartbeatAction: string;
  heartbeatIntervalMs: number;
  onJsonMessage: (message: RawRealtimeMessage) => void;
  onLog?: (entry: Omit<EventLogEntry, 'id' | 'timestamp'>) => void;
  maxReconnectDelayMs?: number;
}

interface UseRealtimeWebSocketResult {
  status: ConnectionStatus;
  activeTopics: string[];
  lastConnectedAt?: string;
  lastMessageAt?: string;
  reconnectAttempt: number;
  error?: string;
  connect: () => void;
  disconnect: () => void;
  reconnect: () => void;
  subscribe: (topic: string) => void;
  unsubscribe: (topic: string) => void;
  sendJson: (payload: Record<string, unknown>) => boolean;
}

const RECONNECT_BASE_DELAY_MS = 700;
const DEFAULT_MAX_RECONNECT_DELAY_MS = 20_000;

const buildBackoffDelay = (attempt: number, maxDelayMs: number): number => {
  const exponential = Math.min(maxDelayMs, RECONNECT_BASE_DELAY_MS * 2 ** Math.max(0, attempt - 1));
  const jitter = Math.round(Math.random() * 450);
  return exponential + jitter;
};

const uniqueTopics = (topics: string[]): string[] => {
  return Array.from(new Set(topics.map((topic) => topic.trim()).filter(Boolean)));
};

export const useRealtimeWebSocket = ({
  url,
  enabled,
  defaultTopics,
  heartbeatAction,
  heartbeatIntervalMs,
  onJsonMessage,
  onLog,
  maxReconnectDelayMs = DEFAULT_MAX_RECONNECT_DELAY_MS,
}: UseRealtimeWebSocketOptions): UseRealtimeWebSocketResult => {
  const [status, setStatus] = useState<ConnectionStatus>('idle');
  const [activeTopics, setActiveTopics] = useState<string[]>(() => uniqueTopics(defaultTopics));
  const [lastConnectedAt, setLastConnectedAt] = useState<string | undefined>();
  const [lastMessageAt, setLastMessageAt] = useState<string | undefined>();
  const [reconnectAttempt, setReconnectAttempt] = useState(0);
  const [error, setError] = useState<string | undefined>();

  const socketRef = useRef<WebSocket | null>(null);
  const heartbeatTimerRef = useRef<number | undefined>();
  const reconnectTimerRef = useRef<number | undefined>();
  const shouldReconnectRef = useRef(false);
  const manuallyClosedRef = useRef(false);
  const onJsonMessageRef = useRef(onJsonMessage);
  const onLogRef = useRef(onLog);
  const topicsRef = useRef<Set<string>>(new Set(uniqueTopics(defaultTopics)));
  const connectRef = useRef<() => void>(() => undefined);

  onJsonMessageRef.current = onJsonMessage;
  onLogRef.current = onLog;

  const log = useCallback((entry: Omit<EventLogEntry, 'id' | 'timestamp'>) => {
    onLogRef.current?.(entry);
  }, []);

  const clearTimers = useCallback(() => {
    if (heartbeatTimerRef.current) {
      window.clearInterval(heartbeatTimerRef.current);
      heartbeatTimerRef.current = undefined;
    }
    if (reconnectTimerRef.current) {
      window.clearTimeout(reconnectTimerRef.current);
      reconnectTimerRef.current = undefined;
    }
  }, []);

  const sendJson = useCallback((payload: Record<string, unknown>): boolean => {
    const socket = socketRef.current;

    if (!socket || socket.readyState !== WebSocket.OPEN) {
      return false;
    }

    socket.send(JSON.stringify(payload));
    return true;
  }, []);

  const resubscribeAll = useCallback(() => {
    topicsRef.current.forEach((topic) => {
      sendJson({ action: 'subscribe', topic });
    });

    if (topicsRef.current.size > 0) {
      log({
        level: 'info',
        message: `Resubscribed to ${topicsRef.current.size} topic(s)`,
        details: Array.from(topicsRef.current).join(', '),
      });
    }
  }, [log, sendJson]);

  const startHeartbeat = useCallback(() => {
    if (!heartbeatAction || heartbeatIntervalMs <= 0) return;

    if (heartbeatTimerRef.current) {
      window.clearInterval(heartbeatTimerRef.current);
    }

    heartbeatTimerRef.current = window.setInterval(() => {
      sendJson({
        action: heartbeatAction,
        client_type: 'dashboard',
        sent_at: new Date().toISOString(),
      });
    }, heartbeatIntervalMs);
  }, [heartbeatAction, heartbeatIntervalMs, sendJson]);

  const scheduleReconnect = useCallback(() => {
    if (!shouldReconnectRef.current || manuallyClosedRef.current || !enabled || !url) return;

    setReconnectAttempt((previous) => {
      const nextAttempt = previous + 1;
      const delayMs = buildBackoffDelay(nextAttempt, maxReconnectDelayMs);

      setStatus('reconnecting');
      log({
        level: 'warning',
        message: `WebSocket reconnect scheduled in ${Math.round(delayMs / 1000)}s`,
        details: `Attempt ${nextAttempt}`,
      });

      reconnectTimerRef.current = window.setTimeout(() => {
        connectRef.current();
      }, delayMs);

      return nextAttempt;
    });
  }, [enabled, log, maxReconnectDelayMs, url]);

  const closeSocket = useCallback(() => {
    const socket = socketRef.current;
    if (!socket) return;

    socket.onopen = null;
    socket.onclose = null;
    socket.onerror = null;
    socket.onmessage = null;

    if (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING) {
      socket.close(1000, 'Client closing connection');
    }

    socketRef.current = null;
  }, []);

  const connect = useCallback(() => {
    if (!enabled || !url) {
      setStatus('idle');
      return;
    }

    clearTimers();
    closeSocket();

    manuallyClosedRef.current = false;
    shouldReconnectRef.current = true;
    setStatus((previous) => (previous === 'reconnecting' ? 'reconnecting' : 'connecting'));
    setError(undefined);

    const socket = new WebSocket(url);
    socketRef.current = socket;

    socket.onopen = () => {
      setStatus('connected');
      setReconnectAttempt(0);
      setError(undefined);
      setLastConnectedAt(new Date().toISOString());

      log({ level: 'success', message: 'WebSocket connected', details: url });
      resubscribeAll();
      startHeartbeat();
    };

    socket.onmessage = (event: MessageEvent<string>) => {
      setLastMessageAt(new Date().toISOString());
      const parsed = parseJsonMessage(event);

      if (!parsed) {
        log({ level: 'warning', message: 'Received non-JSON WebSocket message', details: String(event.data).slice(0, 160) });
        return;
      }

      onJsonMessageRef.current(parsed);
    };

    socket.onerror = () => {
      setError('WebSocket error');
      log({ level: 'error', message: 'WebSocket error', details: 'The browser did not expose a detailed error. Check API Gateway logs.' });
    };

    socket.onclose = (event) => {
      if (heartbeatTimerRef.current) {
        window.clearInterval(heartbeatTimerRef.current);
        heartbeatTimerRef.current = undefined;
      }

      const cleanClose = event.code === 1000 || event.code === 1001;
      log({
        level: cleanClose ? 'info' : 'warning',
        message: `WebSocket closed (${event.code})`,
        details: event.reason || 'No close reason',
      });

      if (manuallyClosedRef.current || !shouldReconnectRef.current) {
        setStatus('disconnected');
        return;
      }

      scheduleReconnect();
    };
  }, [clearTimers, closeSocket, enabled, log, resubscribeAll, scheduleReconnect, startHeartbeat, url]);

  connectRef.current = connect;

  const disconnect = useCallback(() => {
    manuallyClosedRef.current = true;
    shouldReconnectRef.current = false;
    clearTimers();
    closeSocket();
    setStatus('disconnected');
    log({ level: 'info', message: 'WebSocket disconnected by user' });
  }, [clearTimers, closeSocket, log]);

  const reconnect = useCallback(() => {
    log({ level: 'info', message: 'Manual reconnect requested' });
    manuallyClosedRef.current = false;
    shouldReconnectRef.current = true;
    setReconnectAttempt(0);
    connect();
  }, [connect, log]);

  const subscribe = useCallback((topic: string) => {
    const cleanTopic = topic.trim();
    if (!cleanTopic) return;

    topicsRef.current.add(cleanTopic);
    setActiveTopics(Array.from(topicsRef.current));
    const sent = sendJson({ action: 'subscribe', topic: cleanTopic });

    log({
      level: sent ? 'success' : 'info',
      message: sent ? `Subscribed to ${cleanTopic}` : `Topic queued: ${cleanTopic}`,
    });
  }, [log, sendJson]);

  const unsubscribe = useCallback((topic: string) => {
    const cleanTopic = topic.trim();
    if (!cleanTopic) return;

    topicsRef.current.delete(cleanTopic);
    setActiveTopics(Array.from(topicsRef.current));
    const sent = sendJson({ action: 'unsubscribe', topic: cleanTopic });

    log({
      level: sent ? 'success' : 'info',
      message: sent ? `Unsubscribed from ${cleanTopic}` : `Removed queued topic: ${cleanTopic}`,
    });
  }, [log, sendJson]);

  useEffect(() => {
    const cleanTopics = uniqueTopics(defaultTopics);
    topicsRef.current = new Set(cleanTopics);
    setActiveTopics(cleanTopics);
  }, [defaultTopics.join('|')]);

  useEffect(() => {
    if (enabled && url) {
      connect();
    } else {
      disconnect();
      setStatus('idle');
    }

    return () => {
      shouldReconnectRef.current = false;
      manuallyClosedRef.current = true;
      clearTimers();
      closeSocket();
    };
  }, [clearTimers, closeSocket, connect, disconnect, enabled, url]);

  return useMemo(
    () => ({
      status,
      activeTopics,
      lastConnectedAt,
      lastMessageAt,
      reconnectAttempt,
      error,
      connect,
      disconnect,
      reconnect,
      subscribe,
      unsubscribe,
      sendJson,
    }),
    [activeTopics, connect, disconnect, error, lastConnectedAt, lastMessageAt, reconnect, reconnectAttempt, sendJson, status, subscribe, unsubscribe],
  );
};

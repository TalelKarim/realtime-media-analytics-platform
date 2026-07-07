import { useCallback, useEffect, useRef, useState } from 'react';
import { parseJsonMessage } from '../lib/normalize';
import type { ConnectionStatus, EventLogEntry, RawRealtimeMessage } from '../types/realtime';

interface UseRealtimeWebSocketOptions {
  url: string;
  enabled: boolean;
  defaultTopics: string[];
  heartbeatAction: string;
  heartbeatIntervalMs: number;
  onJsonMessage: (message: RawRealtimeMessage) => void;
  onLog: (entry: Omit<EventLogEntry, 'id' | 'timestamp'>) => void;
  maxReconnectDelayMs?: number;
}

interface UseRealtimeWebSocketResult {
  status: ConnectionStatus;
  activeTopics: string[];
  lastConnectedAt?: string;
  lastMessageAt?: string;
  reconnectAttempt: number;
  error?: string;
  sendJson: (message: Record<string, unknown>) => boolean;
  subscribe: (topic: string) => void;
  unsubscribe: (topic: string) => void;
  reconnect: () => void;
  disconnect: () => void;
}

const uniqueTopics = (topics: string[]): string[] => Array.from(new Set(topics.map((topic) => topic.trim()).filter(Boolean)));

const calculateBackoff = (attempt: number, maxDelayMs: number): number => {
  const baseDelayMs = Math.min(1000 * 2 ** Math.max(0, attempt - 1), maxDelayMs);
  const jitterMs = Math.floor(Math.random() * 500);
  return baseDelayMs + jitterMs;
};

export const useRealtimeWebSocket = ({
  url,
  enabled,
  defaultTopics,
  heartbeatAction,
  heartbeatIntervalMs,
  onJsonMessage,
  onLog,
  maxReconnectDelayMs = 30_000,
}: UseRealtimeWebSocketOptions): UseRealtimeWebSocketResult => {
  const [status, setStatus] = useState<ConnectionStatus>('idle');
  const [activeTopics, setActiveTopics] = useState<string[]>(() => uniqueTopics(defaultTopics));
  const [lastConnectedAt, setLastConnectedAt] = useState<string>();
  const [lastMessageAt, setLastMessageAt] = useState<string>();
  const [reconnectAttempt, setReconnectAttempt] = useState(0);
  const [error, setError] = useState<string>();

  const socketRef = useRef<WebSocket | null>(null);
  const heartbeatTimerRef = useRef<number | null>(null);
  const reconnectTimerRef = useRef<number | null>(null);
  const manuallyClosedRef = useRef(false);
  const shouldReconnectRef = useRef(true);
  const activeTopicsRef = useRef<string[]>(uniqueTopics(defaultTopics));
  const connectRef = useRef<() => void>(() => undefined);
  const onJsonMessageRef = useRef(onJsonMessage);
  const onLogRef = useRef(onLog);

  useEffect(() => {
    onJsonMessageRef.current = onJsonMessage;
  }, [onJsonMessage]);

  useEffect(() => {
    onLogRef.current = onLog;
  }, [onLog]);

  useEffect(() => {
    const nextTopics = uniqueTopics(defaultTopics);
    activeTopicsRef.current = nextTopics;
    setActiveTopics(nextTopics);
  }, [defaultTopics]);

  const log = useCallback((entry: Omit<EventLogEntry, 'id' | 'timestamp'>) => {
    onLogRef.current(entry);
  }, []);

  const clearHeartbeat = useCallback(() => {
    if (heartbeatTimerRef.current !== null) {
      window.clearInterval(heartbeatTimerRef.current);
      heartbeatTimerRef.current = null;
    }
  }, []);

  const clearReconnect = useCallback(() => {
    if (reconnectTimerRef.current !== null) {
      window.clearTimeout(reconnectTimerRef.current);
      reconnectTimerRef.current = null;
    }
  }, []);

  const clearTimers = useCallback(() => {
    clearHeartbeat();
    clearReconnect();
  }, [clearHeartbeat, clearReconnect]);

  const sendJson = useCallback((message: Record<string, unknown>): boolean => {
    const socket = socketRef.current;
    if (!socket || socket.readyState !== WebSocket.OPEN) {
      return false;
    }

    socket.send(JSON.stringify(message));
    return true;
  }, []);

  const resubscribeAll = useCallback(() => {
    for (const topic of activeTopicsRef.current) {
      sendJson({ action: 'subscribe', topic });
    }
  }, [sendJson]);

  const startHeartbeat = useCallback(() => {
    clearHeartbeat();

    if (!heartbeatAction || heartbeatIntervalMs <= 0) return;

    heartbeatTimerRef.current = window.setInterval(() => {
      sendJson({
        action: heartbeatAction,
        client_type: 'dashboard',
        sent_at: new Date().toISOString(),
        topics: activeTopicsRef.current,
      });
    }, heartbeatIntervalMs);
  }, [clearHeartbeat, heartbeatAction, heartbeatIntervalMs, sendJson]);

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

  const scheduleReconnect = useCallback(() => {
    if (!enabled || !url || manuallyClosedRef.current || !shouldReconnectRef.current) return;

    setReconnectAttempt((previousAttempt) => {
      const nextAttempt = previousAttempt + 1;
      const delayMs = calculateBackoff(nextAttempt, maxReconnectDelayMs);

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

  const connect = useCallback(() => {
    if (!enabled || !url) {
      setStatus('idle');
      return;
    }

    if (!url.startsWith('wss://') && !url.startsWith('ws://')) {
      setStatus('error');
      setError('Invalid WebSocket URL. It must start with wss:// or ws://');
      return;
    }

    clearTimers();
    closeSocket();

    manuallyClosedRef.current = false;
    shouldReconnectRef.current = true;
    setStatus((previousStatus) => (previousStatus === 'reconnecting' ? 'reconnecting' : 'connecting'));
    setError(undefined);

    let socket: WebSocket;
    try {
      socket = new WebSocket(url);
    } catch (exception) {
      const message = exception instanceof Error ? exception.message : 'Unable to construct WebSocket';
      setStatus('error');
      setError(message);
      log({ level: 'error', message: 'Invalid WebSocket URL', details: message });
      return;
    }

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
      setError('WebSocket error. Check browser Network frames and API Gateway logs.');
      log({ level: 'error', message: 'WebSocket error', details: 'The browser did not expose a detailed error.' });
    };

    socket.onclose = (event: CloseEvent) => {
      clearHeartbeat();

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
  }, [clearHeartbeat, clearTimers, closeSocket, enabled, log, resubscribeAll, scheduleReconnect, startHeartbeat, url]);

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
    manuallyClosedRef.current = false;
    shouldReconnectRef.current = true;
    setReconnectAttempt(0);
    connectRef.current();
  }, []);

  const subscribe = useCallback((topic: string) => {
    const cleanTopic = topic.trim();
    if (!cleanTopic) return;

    setActiveTopics((previousTopics) => {
      const nextTopics = uniqueTopics([...previousTopics, cleanTopic]);
      activeTopicsRef.current = nextTopics;
      return nextTopics;
    });

    const sent = sendJson({ action: 'subscribe', topic: cleanTopic });
    log({ level: sent ? 'success' : 'info', message: `Subscribed to ${cleanTopic}`, details: sent ? 'Subscribe sent over WSS' : 'Will subscribe after connection opens' });
  }, [log, sendJson]);

  const unsubscribe = useCallback((topic: string) => {
    const cleanTopic = topic.trim();
    if (!cleanTopic) return;

    setActiveTopics((previousTopics) => {
      const nextTopics = previousTopics.filter((candidate) => candidate !== cleanTopic);
      activeTopicsRef.current = nextTopics;
      return nextTopics;
    });

    const sent = sendJson({ action: 'unsubscribe', topic: cleanTopic });
    log({ level: sent ? 'info' : 'warning', message: `Unsubscribed from ${cleanTopic}`, details: sent ? 'Unsubscribe sent over WSS' : 'Socket not open; local topic removed' });
  }, [log, sendJson]);

  useEffect(() => {
    if (!enabled) {
      disconnect();
      return;
    }

    connect();

    return () => {
      shouldReconnectRef.current = false;
      clearTimers();
      closeSocket();
    };
  }, [clearTimers, closeSocket, connect, disconnect, enabled]);

  return {
    status,
    activeTopics,
    lastConnectedAt,
    lastMessageAt,
    reconnectAttempt,
    error,
    sendJson,
    subscribe,
    unsubscribe,
    reconnect,
    disconnect,
  };
};

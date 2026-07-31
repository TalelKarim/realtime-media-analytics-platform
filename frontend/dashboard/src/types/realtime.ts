export type ConnectionStatus = 'idle' | 'connecting' | 'connected' | 'reconnecting' | 'disconnected' | 'error';

export type TopicName = string;

export interface ChartPoint {
  name: string;
  count: number;
}

export interface TopPage {
  wiki: string;
  title: string;
  namespace: number | null;
  count: number;
  botCount: number;
  humanCount: number;
}

export interface BroadcastCursor {
  sequence: number;
  aggregationWindowEpochMs: number;
  manifestId?: string;
}

export interface StatsSnapshot {
  topic: TopicName;
  receivedAt: string;
  eventCount: number;
  botCount: number;
  humanCount: number;
  topWikis: ChartPoint[];
  changeTypes: ChartPoint[];
  namespaces: ChartPoint[];
  topPages: TopPage[];
  sequence?: number;
  aggregationWindow?: string;
  manifestId?: string;
  raw?: unknown;
}

export interface RawRealtimeMessage {
  type?: string;
  action?: string;
  topic?: string;
  data?: unknown;
  payload?: unknown;
  updates?: unknown;
  sequence?: unknown;
  manifest_id?: unknown;
  aggregation_window?: unknown;
  aggregation_window_epoch_ms?: unknown;
  chunk_index?: unknown;
  chunk_count?: unknown;
  message?: string;
  error?: string;
  [key: string]: unknown;
}

export interface EventLogEntry {
  id: string;
  level: 'info' | 'success' | 'warning' | 'error';
  timestamp: string;
  message: string;
  details?: string;
}

export interface DashboardSettings {
  wsUrl: string;
  defaultTopics: string[];
  heartbeatAction: string;
  heartbeatIntervalMs: number;
}

export type ConnectionStatus = 'idle' | 'connecting' | 'connected' | 'reconnecting' | 'disconnected' | 'error';

export type TopicName = string;

export interface ChartPoint {
  name: string;
  count: number;
}

export interface TopPage {
  wiki: string;
  title: string;
  namespace?: number | null;
  count: number;
  botCount?: number;
  humanCount?: number;
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
  raw?: unknown;
}

export interface RawRealtimeMessage {
  type?: string;
  action?: string;
  topic?: string;
  data?: unknown;
  payload?: unknown;
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

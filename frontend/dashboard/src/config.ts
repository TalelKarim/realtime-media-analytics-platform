import type { DashboardSettings } from './types/realtime';

const parseTopics = (value: string | undefined): string[] => {
  if (!value) return ['global'];
  const topics = value
    .split(',')
    .map((topic) => topic.trim())
    .filter(Boolean);

  return topics.length > 0 ? topics : ['global'];
};

const parseNumber = (value: string | undefined, fallback: number): number => {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
};

export const DEFAULT_SETTINGS: DashboardSettings = {
  wsUrl: import.meta.env.VITE_WS_URL ?? '',
  defaultTopics: parseTopics(import.meta.env.VITE_DEFAULT_TOPICS),
  heartbeatAction: import.meta.env.VITE_HEARTBEAT_ACTION ?? 'heartbeat',
  heartbeatIntervalMs: parseNumber(import.meta.env.VITE_HEARTBEAT_INTERVAL_MS, 240_000),
};

export const ENABLE_DEMO_DATA = (import.meta.env.VITE_ENABLE_DEMO_DATA ?? 'false').toLowerCase() === 'true';

export const LOCAL_STORAGE_KEYS = {
  settings: 'rma.dashboard.settings',
} as const;

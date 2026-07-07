import type { DashboardSettings } from './types/realtime';

export const cleanString = (value: string | undefined | null): string => {
  if (!value) return '';
  return value.trim().replace(/^['"]|['"]$/g, '').trim();
};

export const sanitizeWebSocketUrl = (value: string | undefined | null): string => {
  const cleaned = cleanString(value);
  if (!cleaned) return '';
  return cleaned.replace(/\/+$/, '');
};

const parseTopics = (value: string | undefined): string[] => {
  const raw = cleanString(value);
  if (!raw) return ['global'];

  const topics = raw
    .split(',')
    .map((topic) => topic.trim())
    .filter(Boolean);

  return topics.length > 0 ? Array.from(new Set(topics)) : ['global'];
};

const parseNumber = (value: string | undefined, fallback: number): number => {
  const parsed = Number(cleanString(value));
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
};

export const DEFAULT_SETTINGS: DashboardSettings = {
  wsUrl: sanitizeWebSocketUrl(import.meta.env.VITE_WS_URL),
  defaultTopics: parseTopics(import.meta.env.VITE_DEFAULT_TOPICS),
  heartbeatAction: cleanString(import.meta.env.VITE_HEARTBEAT_ACTION) || 'heartbeat',
  heartbeatIntervalMs: parseNumber(import.meta.env.VITE_HEARTBEAT_INTERVAL_MS, 240_000),
};

export const ENABLE_DEMO_DATA = cleanString(import.meta.env.VITE_ENABLE_DEMO_DATA).toLowerCase() === 'true';

export const LOCAL_STORAGE_KEYS = {
  settings: 'rma.dashboard.settings',
} as const;

export const SUGGESTED_TOPICS = ['global', 'top_pages', 'wiki:frwiki', 'wiki:enwiki', 'wiki:commonswiki', 'wiki:wikidatawiki'] as const;

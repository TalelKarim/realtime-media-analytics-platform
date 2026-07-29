import type {
  BroadcastCursor,
  ChartPoint,
  RawRealtimeMessage,
  StatsSnapshot,
  TopPage,
} from '../types/realtime';

const isRecord = (value: unknown): value is Record<string, unknown> => {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
};

const toNumber = (value: unknown, fallback = 0): number => {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value === 'string') {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  return fallback;
};

const readAny = (record: Record<string, unknown>, keys: string[]): unknown => {
  for (const key of keys) {
    const value = record[key];
    if (value !== undefined && value !== null) return value;
  }
  return undefined;
};

const normalizeChartPoints = (value: unknown): ChartPoint[] => {
  if (Array.isArray(value)) {
    const points: ChartPoint[] = [];
    for (const item of value) {
      if (!isRecord(item)) continue;
      const name = String(readAny(item, ['name', 'wiki', 'change_type', 'changeType', 'namespace', 'key', 'label', 'type']) ?? 'unknown');
      const count = toNumber(readAny(item, ['count', 'event_count', 'eventCount', 'value', 'events']));
      points.push({ name, count });
    }
    return points.sort((a, b) => b.count - a.count);
  }

  if (isRecord(value)) {
    return Object.entries(value)
      .map(([name, count]) => ({ name, count: toNumber(count) }))
      .sort((a, b) => b.count - a.count);
  }
  return [];
};

const normalizeTopPages = (value: unknown): TopPage[] => {
  if (!Array.isArray(value)) return [];
  const pages: TopPage[] = [];
  for (const item of value) {
    if (!isRecord(item)) continue;
    const namespaceValue = readAny(item, ['namespace', 'ns']);
    pages.push({
      wiki: String(readAny(item, ['wiki']) ?? 'unknown'),
      title: String(readAny(item, ['title', 'page', 'name']) ?? 'unknown'),
      namespace: namespaceValue === undefined || namespaceValue === null ? null : toNumber(namespaceValue),
      count: toNumber(readAny(item, ['count', 'event_count', 'eventCount', 'value', 'events'])),
      botCount: toNumber(readAny(item, ['bot_count', 'botCount', 'bot_event_count']), 0),
      humanCount: toNumber(readAny(item, ['human_count', 'humanCount', 'human_event_count']), 0),
    });
  }
  return pages.sort((a, b) => b.count - a.count);
};

const isoToEpochMs = (value: unknown): number => {
  if (typeof value !== 'string' || !value.trim()) return 0;
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : 0;
};

export const extractBroadcastCursor = (message: RawRealtimeMessage): BroadcastCursor | null => {
  const sequence = toNumber(message.sequence, 0);
  if (sequence <= 0) return null;
  const aggregationWindowEpochMs = toNumber(
    message.aggregation_window_epoch_ms,
    isoToEpochMs(message.aggregation_window),
  );
  return {
    sequence,
    aggregationWindowEpochMs,
    manifestId: typeof message.manifest_id === 'string' ? message.manifest_id : undefined,
  };
};

export const compareBroadcastCursors = (left: BroadcastCursor, right: BroadcastCursor): number => {
  if (left.sequence !== right.sequence) return left.sequence - right.sequence;
  return left.aggregationWindowEpochMs - right.aggregationWindowEpochMs;
};

const normalizeSingleRealtimeMessage = (message: RawRealtimeMessage): StatsSnapshot | null => {
  const body = isRecord(message.data)
    ? message.data
    : isRecord(message.payload)
      ? message.payload
      : isRecord(message)
        ? message
        : null;

  if (!body) return null;
  const messageType = String(message.type ?? message.action ?? body.type ?? '').toLowerCase();
  const looksLikeStats =
    messageType.includes('stats') ||
    body.top_wikis !== undefined ||
    body.topWikis !== undefined ||
    body.event_count !== undefined ||
    body.eventCount !== undefined ||
    body.events_count !== undefined ||
    body.eventsCount !== undefined ||
    body.current_minute_events_so_far !== undefined;
  if (!looksLikeStats) return null;

  const topic = String(message.topic ?? body.topic ?? 'global');
  const botCount = toNumber(readAny(body, ['bot_count', 'botCount', 'bot_event_count', 'botEvents']));
  const humanCount = toNumber(readAny(body, ['human_count', 'humanCount', 'human_event_count', 'humanEvents']));
  const eventCount = toNumber(
    readAny(body, [
      'event_count',
      'eventCount',
      'events_count',
      'eventsCount',
      'current_minute_events',
      'current_minute_events_so_far',
      'total_events',
      'totalEvents',
      'count',
    ]),
    botCount + humanCount,
  );

  return {
    topic,
    receivedAt: new Date().toISOString(),
    eventCount,
    botCount,
    humanCount,
    topWikis: normalizeChartPoints(readAny(body, ['top_wikis', 'topWikis', 'wiki_activity', 'wikis'])),
    changeTypes: normalizeChartPoints(readAny(body, ['change_types', 'changeTypes', 'change_type_distribution', 'changeTypeDistribution'])),
    namespaces: normalizeChartPoints(readAny(body, ['namespaces', 'namespace_distribution', 'namespaceDistribution'])),
    topPages: normalizeTopPages(readAny(body, ['top_pages', 'topPages', 'pages'])),
    sequence: toNumber(message.sequence, 0) || undefined,
    aggregationWindow: typeof message.aggregation_window === 'string' ? message.aggregation_window : undefined,
    manifestId: typeof message.manifest_id === 'string' ? message.manifest_id : undefined,
    raw: message,
  };
};

export const normalizeRealtimeMessages = (message: RawRealtimeMessage): StatsSnapshot[] => {
  const messageType = String(message.type ?? message.action ?? '').toLowerCase();
  if (messageType === 'stats.batch_update' && Array.isArray(message.updates)) {
    const snapshots: StatsSnapshot[] = [];
    for (const rawUpdate of message.updates) {
      if (!isRecord(rawUpdate)) continue;
      const topic = String(rawUpdate.topic ?? '').trim();
      if (!topic || !isRecord(rawUpdate.data)) continue;
      const normalized = normalizeSingleRealtimeMessage({
        type: 'stats.update',
        topic,
        data: rawUpdate.data,
        sequence: message.sequence,
        manifest_id: message.manifest_id,
        aggregation_window: message.aggregation_window,
        aggregation_window_epoch_ms: message.aggregation_window_epoch_ms,
      });
      if (normalized) snapshots.push(normalized);
    }
    return snapshots;
  }

  const normalized = normalizeSingleRealtimeMessage(message);
  return normalized ? [normalized] : [];
};

// Backward-compatible helper retained for any existing import.
export const normalizeRealtimeMessage = (message: RawRealtimeMessage): StatsSnapshot | null => {
  return normalizeRealtimeMessages(message)[0] ?? null;
};

export const parseJsonMessage = (raw: MessageEvent<string>): RawRealtimeMessage | null => {
  try {
    const parsed = JSON.parse(raw.data) as unknown;
    return isRecord(parsed) ? (parsed as RawRealtimeMessage) : null;
  } catch {
    return null;
  }
};

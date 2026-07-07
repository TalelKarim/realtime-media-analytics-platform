import type { ChartPoint, RawRealtimeMessage, StatsSnapshot, TopPage } from '../types/realtime';

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
    if (record[key] !== undefined && record[key] !== null) return record[key];
  }
  return undefined;
};

const normalizeChartPoints = (value: unknown): ChartPoint[] => {
  if (Array.isArray(value)) {
    return value
      .map((item) => {
        if (!isRecord(item)) return null;
        const name = String(readAny(item, ['name', 'wiki', 'change_type', 'changeType', 'namespace', 'key', 'label', 'type']) ?? 'unknown');
        const count = toNumber(readAny(item, ['count', 'event_count', 'eventCount', 'value', 'events']));
        return { name, count } satisfies ChartPoint;
      })
      .filter((item): item is ChartPoint => Boolean(item))
      .sort((a, b) => b.count - a.count);
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

  return value
    .map((item) => {
      if (!isRecord(item)) return null;

      return {
        wiki: String(readAny(item, ['wiki']) ?? 'unknown'),
        title: String(readAny(item, ['title', 'page', 'name']) ?? 'unknown'),
        namespace: readAny(item, ['namespace']) === undefined ? null : toNumber(readAny(item, ['namespace'])),
        count: toNumber(readAny(item, ['count', 'event_count', 'eventCount', 'value', 'events'])),
        botCount: toNumber(readAny(item, ['bot_count', 'botCount', 'bot_event_count']), 0),
        humanCount: toNumber(readAny(item, ['human_count', 'humanCount', 'human_event_count']), 0),
      } satisfies TopPage;
    })
    .filter((item): item is TopPage => Boolean(item))
    .sort((a, b) => b.count - a.count);
};

export const normalizeRealtimeMessage = (message: RawRealtimeMessage): StatsSnapshot | null => {
  const body = isRecord(message.data)
    ? message.data
    : isRecord(message.payload)
      ? message.payload
      : isRecord(message)
        ? message
        : null;

  if (!body) return null;

  const messageType = String(message.type ?? message.action ?? body.type ?? '').toLowerCase();
  const looksLikeStats = messageType.includes('stats') || body.top_wikis !== undefined || body.topWikis !== undefined || body.event_count !== undefined || body.events_count !== undefined;

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

  const topWikis = normalizeChartPoints(readAny(body, ['top_wikis', 'topWikis', 'wiki_activity', 'wikis']));
  const changeTypes = normalizeChartPoints(readAny(body, ['change_types', 'changeTypes', 'change_type_distribution', 'changeTypeDistribution']));
  const namespaces = normalizeChartPoints(readAny(body, ['namespaces', 'namespace_distribution', 'namespaceDistribution']));
  const topPages = normalizeTopPages(readAny(body, ['top_pages', 'topPages', 'pages']));

  return {
    topic,
    receivedAt: new Date().toISOString(),
    eventCount,
    botCount,
    humanCount,
    topWikis,
    changeTypes,
    namespaces,
    topPages,
    raw: message,
  };
};

export const parseJsonMessage = (raw: MessageEvent<string>): RawRealtimeMessage | null => {
  try {
    const parsed = JSON.parse(raw.data) as unknown;
    return isRecord(parsed) ? (parsed as RawRealtimeMessage) : null;
  } catch {
    return null;
  }
};

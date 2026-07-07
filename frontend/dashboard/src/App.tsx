import { useCallback, useEffect, useMemo, useState } from 'react';
import { BarChart3, Bot, BrainCircuit, Globe2, RadioTower, Settings2, Users } from 'lucide-react';
import { ConnectionStatus } from './components/ConnectionStatus';
import { SettingsPanel } from './components/SettingsPanel';
import { TopicSelector } from './components/TopicSelector';
import { MetricCard } from './components/MetricCard';
import { ChartCard } from './components/ChartCard';
import { EventLog } from './components/EventLog';
import { EmptyState } from './components/EmptyState';
import { TopWikisChart } from './components/charts/TopWikisChart';
import { ChangeTypeChart } from './components/charts/ChangeTypeChart';
import { BotHumanDonut } from './components/charts/BotHumanDonut';
import { NamespaceChart } from './components/charts/NamespaceChart';
import { TopPagesTable } from './components/charts/TopPagesTable';
import { DEFAULT_SETTINGS, ENABLE_DEMO_DATA, LOCAL_STORAGE_KEYS, sanitizeWebSocketUrl } from './config';
import { demoStats } from './lib/demoData';
import { formatNumber, formatTime, topicLabel } from './lib/format';
import { normalizeRealtimeMessage } from './lib/normalize';
import { useRealtimeWebSocket } from './hooks/useRealtimeWebSocket';
import type { DashboardSettings, EventLogEntry, RawRealtimeMessage, StatsSnapshot } from './types/realtime';

const createLogId = (): string => {
  if ('crypto' in window && typeof window.crypto.randomUUID === 'function') {
    return window.crypto.randomUUID();
  }
  return `${Date.now()}-${Math.random().toString(16).slice(2)}`;
};

const sanitizeTopics = (topics: string[] | undefined): string[] => {
  const safeTopics = (topics ?? []).map((topic) => topic.trim()).filter(Boolean);
  return safeTopics.length > 0 ? Array.from(new Set(safeTopics)) : ['global'];
};

const loadSettings = (): DashboardSettings => {
  try {
    const stored = window.localStorage.getItem(LOCAL_STORAGE_KEYS.settings);
    if (!stored) return DEFAULT_SETTINGS;

    const parsed = JSON.parse(stored) as Partial<DashboardSettings>;
    return {
      ...DEFAULT_SETTINGS,
      ...parsed,
      wsUrl: sanitizeWebSocketUrl(parsed.wsUrl ?? DEFAULT_SETTINGS.wsUrl),
      defaultTopics: sanitizeTopics(parsed.defaultTopics ?? DEFAULT_SETTINGS.defaultTopics),
      heartbeatAction: parsed.heartbeatAction?.trim() || DEFAULT_SETTINGS.heartbeatAction,
      heartbeatIntervalMs: Number.isFinite(parsed.heartbeatIntervalMs) && Number(parsed.heartbeatIntervalMs) > 0
        ? Number(parsed.heartbeatIntervalMs)
        : DEFAULT_SETTINGS.heartbeatIntervalMs,
    };
  } catch {
    return DEFAULT_SETTINGS;
  }
};

const createInitialStats = (): Record<string, StatsSnapshot> => {
  const initialState: Record<string, StatsSnapshot> = {};
  if (ENABLE_DEMO_DATA) {
    initialState.global = demoStats;
  }
  return initialState;
};

const App = () => {
  const [settings, setSettings] = useState<DashboardSettings>(() => loadSettings());
  const [enabled, setEnabled] = useState(true);
  const [showSettings, setShowSettings] = useState(!settings.wsUrl);
  const [selectedTopic, setSelectedTopic] = useState(settings.defaultTopics[0] ?? 'global');
  const [statsByTopic, setStatsByTopic] = useState<Record<string, StatsSnapshot>>(() => createInitialStats());
  const [logs, setLogs] = useState<EventLogEntry[]>([]);

  useEffect(() => {
    window.localStorage.setItem(LOCAL_STORAGE_KEYS.settings, JSON.stringify(settings));
  }, [settings]);

  const addLog = useCallback((entry: Omit<EventLogEntry, 'id' | 'timestamp'>) => {
    setLogs((previousLogs) => [
      {
        id: createLogId(),
        timestamp: new Date().toISOString(),
        ...entry,
      },
      ...previousLogs,
    ].slice(0, 50));
  }, []);

  const handleJsonMessage = useCallback((message: RawRealtimeMessage) => {
    const normalized = normalizeRealtimeMessage(message);
    const rawType = String(message.type ?? message.action ?? 'message');

    if (message.error || rawType.toLowerCase().includes('error')) {
      addLog({
        level: 'error',
        message: message.error ? String(message.error) : 'Backend returned an error message',
        details: JSON.stringify(message).slice(0, 280),
      });
    }

    if (!normalized) {
      if (!['pong', 'ack', 'subscribed', 'unsubscribed', 'heartbeat.ack'].includes(rawType.toLowerCase())) {
        addLog({ level: 'info', message: `Received ${rawType}`, details: JSON.stringify(message).slice(0, 280) });
      }
      return;
    }

    setStatsByTopic((previousStats) => ({ ...previousStats, [normalized.topic]: normalized }));
    setSelectedTopic((previousTopic) => previousTopic || normalized.topic);
    addLog({
      level: 'success',
      message: `stats.update received for ${normalized.topic}`,
      details: `${formatNumber(normalized.eventCount)} event(s) in latest live window`,
    });
  }, [addLog]);

  const ws = useRealtimeWebSocket({
    url: settings.wsUrl,
    enabled,
    defaultTopics: settings.defaultTopics,
    heartbeatAction: settings.heartbeatAction,
    heartbeatIntervalMs: settings.heartbeatIntervalMs,
    onJsonMessage: handleJsonMessage,
    onLog: addLog,
  });

  const topics = ws.activeTopics.length > 0 ? ws.activeTopics : settings.defaultTopics;

  useEffect(() => {
    if (!selectedTopic && topics.length > 0) {
      setSelectedTopic(topics[0]);
    }
  }, [selectedTopic, topics]);

  const currentStats = useMemo(() => {
    return statsByTopic[selectedTopic] ?? (ENABLE_DEMO_DATA ? demoStats : undefined);
  }, [selectedTopic, statsByTopic]);

  const botPercent = currentStats && currentStats.eventCount > 0 ? (currentStats.botCount / currentStats.eventCount) * 100 : 0;
  const humanPercent = currentStats && currentStats.eventCount > 0 ? (currentStats.humanCount / currentStats.eventCount) * 100 : 0;
  const isWikiTopic = selectedTopic.startsWith('wiki:');
  const activeTopicLabel = topicLabel(selectedTopic || 'global');

  return (
    <main className="min-h-screen bg-slate-950 text-slate-100">
      <div className="pointer-events-none fixed inset-0 overflow-hidden">
        <div className="absolute left-1/2 top-0 h-[38rem] w-[38rem] -translate-x-1/2 rounded-full bg-sky-500/10 blur-3xl" />
        <div className="absolute bottom-0 right-0 h-[34rem] w-[34rem] rounded-full bg-emerald-500/10 blur-3xl" />
      </div>

      <div className="relative mx-auto max-w-7xl px-4 py-8 sm:px-6 lg:px-8">
        <header className="mb-8 flex flex-col gap-5 lg:flex-row lg:items-end lg:justify-between">
          <div>
            <div className="inline-flex items-center gap-2 rounded-full bg-sky-500/10 px-3 py-1 text-sm font-medium text-sky-200 ring-1 ring-sky-500/20">
              <RadioTower className="h-4 w-4" />
              Live WebSocket dashboard
            </div>
            <h1 className="mt-4 text-4xl font-black tracking-tight text-white sm:text-5xl">Realtime Media Analytics</h1>
            <p className="mt-3 max-w-3xl text-base leading-7 text-slate-400">
              Live Wikimedia activity powered by API Gateway WebSocket, Lambda, DynamoDB aggregates and the broadcaster pipeline.
            </p>
            <div className="mt-4 flex flex-wrap gap-2 text-xs text-slate-400">
              <span className="rounded-full bg-slate-900 px-3 py-1 ring-1 ring-slate-800">Current live window</span>
              <span className="rounded-full bg-slate-900 px-3 py-1 ring-1 ring-slate-800">Auto reconnect</span>
              <span className="rounded-full bg-slate-900 px-3 py-1 ring-1 ring-slate-800">Heartbeat TTL refresh</span>
            </div>
          </div>
          <button
            type="button"
            onClick={() => setShowSettings((value) => !value)}
            className="inline-flex items-center justify-center gap-2 rounded-2xl bg-slate-900 px-4 py-3 text-sm font-semibold text-slate-200 ring-1 ring-slate-800 transition hover:bg-slate-800"
          >
            <Settings2 className="h-4 w-4" />
            {showSettings ? 'Hide settings' : 'Settings'}
          </button>
        </header>

        <div className="space-y-6">
          {showSettings && (
            <SettingsPanel
              settings={settings}
              enabled={enabled}
              onSettingsChange={(nextSettings) => {
                setSettings(nextSettings);
                if (nextSettings.defaultTopics.length > 0 && !nextSettings.defaultTopics.includes(selectedTopic)) {
                  setSelectedTopic(nextSettings.defaultTopics[0]);
                }
              }}
              onEnabledChange={setEnabled}
            />
          )}

          <ConnectionStatus
            status={ws.status}
            url={settings.wsUrl}
            lastConnectedAt={ws.lastConnectedAt}
            lastMessageAt={ws.lastMessageAt}
            reconnectAttempt={ws.reconnectAttempt}
            error={ws.error}
            onReconnect={ws.reconnect}
            onDisconnect={ws.disconnect}
          />

          <TopicSelector
            activeTopics={topics}
            selectedTopic={selectedTopic}
            onSelectedTopicChange={setSelectedTopic}
            onSubscribe={ws.subscribe}
            onUnsubscribe={(topic) => {
              ws.unsubscribe(topic);
              if (selectedTopic === topic) {
                const remainingTopics = topics.filter((candidateTopic) => candidateTopic !== topic);
                setSelectedTopic(remainingTopics[0] ?? 'global');
              }
            }}
          />

          <section className="rounded-3xl border border-slate-800/90 bg-slate-950/70 px-5 py-4 text-sm text-slate-400 ring-1 ring-white/5">
            <span className="font-semibold text-slate-200">Selected view:</span> {activeTopicLabel}. Metrics represent the latest live aggregation window, so counters can reset when the backend opens a new window.
          </section>

          <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
            <MetricCard label="Events in current window" value={currentStats?.eventCount ?? 0} icon={BarChart3} helper={`Topic ${selectedTopic || '—'}`} tone="sky" />
            <MetricCard label="Bot events" value={currentStats?.botCount ?? 0} icon={Bot} helper="Automated activity" percent={botPercent} tone="violet" />
            <MetricCard label="Human events" value={currentStats?.humanCount ?? 0} icon={Users} helper="User activity" percent={humanPercent} tone="emerald" />
            <MetricCard label={isWikiTopic ? 'Topic mode' : 'Tracked wikis'} value={isWikiTopic ? 1 : currentStats?.topWikis.length ?? 0} icon={Globe2} helper={`Updated ${formatTime(currentStats?.receivedAt)}`} tone="amber" />
          </div>

          {!currentStats ? (
            <EmptyState topic={selectedTopic || 'global'} />
          ) : (
            <>
              <div className="grid gap-6 xl:grid-cols-2">
                <ChartCard
                  title={isWikiTopic ? 'Selected wiki' : 'Top wikis'}
                  description={isWikiTopic ? 'This view is already filtered to one wiki. Per-wiki breakdowns below depend on the backend payload.' : 'Highest activity by Wikimedia project for the selected live topic.'}
                  badge={selectedTopic}
                >
                  {isWikiTopic ? (
                    <div className="flex h-72 items-center justify-center rounded-2xl bg-slate-900/40 p-6 text-center text-sm leading-6 text-slate-400 ring-1 ring-slate-800">
                      <p>
                        You are viewing <span className="font-mono text-slate-200">{selectedTopic}</span>. Top wikis are only meaningful in the global topic; this card is intentionally contextual.
                      </p>
                    </div>
                  ) : (
                    <TopWikisChart data={currentStats.topWikis} />
                  )}
                </ChartCard>

                <ChartCard title="Bot vs human" description="Share of automated versus human edits/events in the selected live window.">
                  <BotHumanDonut botCount={currentStats.botCount} humanCount={currentStats.humanCount} />
                </ChartCard>
              </div>

              <div className="grid gap-6 xl:grid-cols-2">
                <ChartCard title="Change type distribution" description="Live breakdown of edit, categorize, log, new and other event types.">
                  <ChangeTypeChart data={currentStats.changeTypes} />
                </ChartCard>

                <ChartCard title="Namespace activity" description="Activity by MediaWiki namespace in the selected live window.">
                  <NamespaceChart data={currentStats.namespaces} />
                </ChartCard>
              </div>

              <ChartCard title="Top pages" description="Most active pages in the latest live update." className="xl:col-span-2">
                <TopPagesTable data={currentStats.topPages} />
              </ChartCard>
            </>
          )}

          <div className="grid gap-6 lg:grid-cols-[1fr_360px]">
            <section className="rounded-3xl border border-slate-800/90 bg-slate-950/85 p-5 backdrop-blur">
              <h2 className="text-lg font-semibold text-white">Backend contract health</h2>
              <p className="mt-1 text-sm leading-6 text-slate-400">
                The frontend accepts tolerant field names, but the best production contract is a consistent <span className="font-mono text-slate-200">stats.update</span> message with event counts, bot/human, change types, namespaces and top pages for each topic.
              </p>
              <div className="mt-4 grid gap-3 sm:grid-cols-2">
                <div className="rounded-2xl bg-slate-900/70 p-4 ring-1 ring-slate-800">
                  <p className="text-xs uppercase tracking-wide text-slate-500">Current topic</p>
                  <p className="mt-1 break-all font-mono text-sm text-slate-200">{selectedTopic}</p>
                </div>
                <div className="rounded-2xl bg-slate-900/70 p-4 ring-1 ring-slate-800">
                  <p className="text-xs uppercase tracking-wide text-slate-500">Last payload</p>
                  <p className="mt-1 text-sm text-slate-200">{currentStats ? `${formatNumber(currentStats.eventCount)} events at ${formatTime(currentStats.receivedAt)}` : 'No payload yet'}</p>
                </div>
              </div>
            </section>

            <EventLog entries={logs} />
          </div>
        </div>
      </div>
    </main>
  );
};

export default App;

import { useCallback, useEffect, useMemo, useState } from 'react';
import { BarChart3, Bot, BrainCircuit, Globe2, Users, RadioTower, Settings2 } from 'lucide-react';
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
import { DEFAULT_SETTINGS, ENABLE_DEMO_DATA, LOCAL_STORAGE_KEYS } from './config';
import { demoStats } from './lib/demoData';
import { formatNumber, formatTime } from './lib/format';
import { normalizeRealtimeMessage } from './lib/normalize';
import { useRealtimeWebSocket } from './hooks/useRealtimeWebSocket';
import type { DashboardSettings, EventLogEntry, RawRealtimeMessage, StatsSnapshot } from './types/realtime';

const createLogId = (): string => {
  if ('crypto' in window && typeof window.crypto.randomUUID === 'function') {
    return window.crypto.randomUUID();
  }
  return `${Date.now()}-${Math.random().toString(16).slice(2)}`;
};

const loadSettings = (): DashboardSettings => {
  try {
    const stored = window.localStorage.getItem(LOCAL_STORAGE_KEYS.settings);
    if (!stored) return DEFAULT_SETTINGS;
    const parsed = JSON.parse(stored) as Partial<DashboardSettings>;
    return {
      ...DEFAULT_SETTINGS,
      ...parsed,
      defaultTopics: parsed.defaultTopics?.length ? parsed.defaultTopics : DEFAULT_SETTINGS.defaultTopics,
    };
  } catch {
    return DEFAULT_SETTINGS;
  }
};

const App = () => {
  const [settings, setSettings] = useState<DashboardSettings>(() => loadSettings());
  const [enabled, setEnabled] = useState(true);
  const [showSettings, setShowSettings] = useState(!settings.wsUrl);
  const [selectedTopic, setSelectedTopic] = useState(settings.defaultTopics[0] ?? 'global');
  const [statsByTopic, setStatsByTopic] = useState<Record<string, StatsSnapshot>>(() => ENABLE_DEMO_DATA ? { global: demoStats } : {});
  const [logs, setLogs] = useState<EventLogEntry[]>([]);

  useEffect(() => {
    window.localStorage.setItem(LOCAL_STORAGE_KEYS.settings, JSON.stringify(settings));
  }, [settings]);

  const addLog = useCallback((entry: Omit<EventLogEntry, 'id' | 'timestamp'>) => {
    setLogs((previous) => [
      {
        id: createLogId(),
        timestamp: new Date().toISOString(),
        ...entry,
      },
      ...previous,
    ].slice(0, 40));
  }, []);

  const handleJsonMessage = useCallback((message: RawRealtimeMessage) => {
    const normalized = normalizeRealtimeMessage(message);

    if (message.error || String(message.type ?? '').toLowerCase().includes('error')) {
      addLog({
        level: 'error',
        message: message.error ? String(message.error) : 'Backend returned an error message',
        details: JSON.stringify(message).slice(0, 240),
      });
    }

    if (!normalized) {
      const type = String(message.type ?? message.action ?? 'message');
      if (!['pong', 'ack', 'subscribed', 'unsubscribed'].includes(type.toLowerCase())) {
        addLog({ level: 'info', message: `Received ${type}`, details: JSON.stringify(message).slice(0, 240) });
      }
      return;
    }

    setStatsByTopic((previous) => ({ ...previous, [normalized.topic]: normalized }));
    setSelectedTopic((previous) => previous || normalized.topic);
    addLog({
      level: 'success',
      message: `stats.update received for ${normalized.topic}`,
      details: `${formatNumber(normalized.eventCount)} event(s)`,
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

  return (
    <main className="min-h-screen bg-slate-950 text-slate-100">
      <div className="pointer-events-none fixed inset-0 overflow-hidden">
        <div className="absolute left-1/2 top-0 h-[36rem] w-[36rem] -translate-x-1/2 rounded-full bg-sky-500/10 blur-3xl" />
        <div className="absolute bottom-0 right-0 h-[30rem] w-[30rem] rounded-full bg-emerald-500/10 blur-3xl" />
      </div>

      <div className="relative mx-auto max-w-7xl px-4 py-8 sm:px-6 lg:px-8">
        <header className="mb-8 flex flex-col gap-5 lg:flex-row lg:items-end lg:justify-between">
          <div>
            <div className="inline-flex items-center gap-2 rounded-full bg-sky-500/10 px-3 py-1 text-sm font-medium text-sky-200 ring-1 ring-sky-500/20">
              <RadioTower className="h-4 w-4" />
              Live WebSocket dashboard
            </div>
            <h1 className="mt-4 text-4xl font-black tracking-tight text-white sm:text-5xl">Realtime Media Analytics</h1>
            <p className="mt-3 max-w-3xl text-base text-slate-400">
              Live Wikimedia activity powered by API Gateway WebSocket, Lambda, DynamoDB aggregates and the broadcaster pipeline.
            </p>
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
              onSettingsChange={(next) => {
                setSettings(next);
                if (next.defaultTopics.length > 0 && !next.defaultTopics.includes(selectedTopic)) {
                  setSelectedTopic(next.defaultTopics[0]);
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
                const remaining = topics.filter((candidate) => candidate !== topic);
                setSelectedTopic(remaining[0] ?? 'global');
              }
            }}
          />

          <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
            <MetricCard label="Current events" value={currentStats?.eventCount ?? 0} icon={BarChart3} helper={`Topic ${selectedTopic || '—'}`} />
            <MetricCard label="Bot events" value={currentStats?.botCount ?? 0} icon={Bot} helper="Automated activity" percent={botPercent} />
            <MetricCard label="Human events" value={currentStats?.humanCount ?? 0} icon={Users} helper="User activity" percent={humanPercent} />
            <MetricCard label="Top wikis" value={currentStats?.topWikis.length ?? 0} icon={Globe2} helper={`Updated ${formatTime(currentStats?.receivedAt)}`} />
          </div>

          {!currentStats ? (
            <EmptyState topic={selectedTopic || 'global'} />
          ) : (
            <>
              <div className="grid gap-6 xl:grid-cols-2">
                <ChartCard title="Top wikis" description="Highest activity by Wikimedia project for the selected live topic.">
                  <TopWikisChart data={currentStats.topWikis} />
                </ChartCard>

                <ChartCard title="Bot vs human" description="Share of automated versus human edits/events.">
                  <BotHumanDonut botCount={currentStats.botCount} humanCount={currentStats.humanCount} />
                </ChartCard>
              </div>

              <div className="grid gap-6 xl:grid-cols-2">
                <ChartCard title="Change type distribution" description="Live breakdown of edit, categorize, log, new and other event types.">
                  <ChangeTypeChart data={currentStats.changeTypes} />
                </ChartCard>

                <ChartCard title="Namespace activity" description="Activity by MediaWiki namespace.">
                  <NamespaceChart data={currentStats.namespaces} />
                </ChartCard>
              </div>

              <ChartCard title="Top pages" description="Most active pages in the latest live update." className="xl:col-span-2">
                <TopPagesTable pages={currentStats.topPages} />
              </ChartCard>
            </>
          )}

          <div className="grid gap-6 xl:grid-cols-[1fr_380px]">
            <section className="rounded-3xl border border-slate-800 bg-slate-950/80 p-5 backdrop-blur">
              <div className="mb-4 flex items-center gap-3">
                <div className="rounded-2xl bg-sky-500/10 p-3 text-sky-300 ring-1 ring-sky-500/20">
                  <BrainCircuit className="h-5 w-5" />
                </div>
                <div>
                  <h2 className="text-lg font-semibold text-white">Architecture note</h2>
                  <p className="text-sm text-slate-400">The frontend never writes to DynamoDB directly.</p>
                </div>
              </div>
              <div className="grid gap-3 text-sm text-slate-400 md:grid-cols-3">
                <div className="rounded-2xl bg-slate-900/70 p-4 ring-1 ring-slate-800">
                  <p className="font-semibold text-slate-200">1. Subscribe</p>
                  <p className="mt-2">The browser sends subscribe/unsubscribe actions over WSS.</p>
                </div>
                <div className="rounded-2xl bg-slate-900/70 p-4 ring-1 ring-slate-800">
                  <p className="font-semibold text-slate-200">2. Heartbeat</p>
                  <p className="mt-2">A periodic heartbeat lets the backend refresh the connection TTL.</p>
                </div>
                <div className="rounded-2xl bg-slate-900/70 p-4 ring-1 ring-slate-800">
                  <p className="font-semibold text-slate-200">3. Broadcast</p>
                  <p className="mt-2">Broadcaster Lambda pushes stats.update messages with the latest aggregates.</p>
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

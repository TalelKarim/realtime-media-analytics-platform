import type { ChangeEvent } from 'react';
import { Save } from 'lucide-react';
import { sanitizeWebSocketUrl } from '../config';
import type { DashboardSettings } from '../types/realtime';

interface SettingsPanelProps {
  settings: DashboardSettings;
  enabled: boolean;
  onSettingsChange: (settings: DashboardSettings) => void;
  onEnabledChange: (enabled: boolean) => void;
}

const parseTopicsInput = (value: string): string[] => {
  const topics = value
    .split(',')
    .map((topic) => topic.trim())
    .filter(Boolean);

  return Array.from(new Set(topics));
};

export const SettingsPanel = ({ settings, enabled, onSettingsChange, onEnabledChange }: SettingsPanelProps) => {
  const update = <K extends keyof DashboardSettings>(key: K, value: DashboardSettings[K]) => {
    onSettingsChange({ ...settings, [key]: value });
  };

  return (
    <section className="rounded-3xl border border-slate-800/90 bg-slate-950/85 p-5 backdrop-blur">
      <div className="mb-5 flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h2 className="text-lg font-semibold text-white">Runtime configuration</h2>
          <p className="text-sm text-slate-400">Configure the API Gateway WebSocket endpoint and default subscriptions.</p>
        </div>
        <label className="flex items-center gap-3 text-sm text-slate-300">
          <span>Auto connect</span>
          <input
            type="checkbox"
            checked={enabled}
            onChange={(event: ChangeEvent<HTMLInputElement>) => onEnabledChange(event.target.checked)}
            className="h-4 w-4 rounded border-slate-700 bg-slate-900 text-sky-500 focus:ring-sky-500"
          />
        </label>
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        <label className="space-y-2 lg:col-span-2">
          <span className="text-sm font-medium text-slate-300">WebSocket URL</span>
          <input
            value={settings.wsUrl}
            onChange={(event: ChangeEvent<HTMLInputElement>) => update('wsUrl', sanitizeWebSocketUrl(event.target.value))}
            placeholder="wss://xxxx.execute-api.us-east-1.amazonaws.com/dev"
            className="w-full rounded-2xl border border-slate-800 bg-slate-900/80 px-4 py-3 font-mono text-sm text-slate-100 outline-none transition placeholder:text-slate-600 focus:border-sky-500"
          />
        </label>

        <label className="space-y-2 lg:col-span-2">
          <span className="text-sm font-medium text-slate-300">Default topics</span>
          <input
            value={settings.defaultTopics.join(',')}
            onChange={(event: ChangeEvent<HTMLInputElement>) => update('defaultTopics', parseTopicsInput(event.target.value))}
            placeholder="global,wiki:frwiki,wiki:commonswiki"
            className="w-full rounded-2xl border border-slate-800 bg-slate-900/80 px-4 py-3 font-mono text-sm text-slate-100 outline-none transition placeholder:text-slate-600 focus:border-sky-500"
          />
        </label>

        <label className="space-y-2">
          <span className="text-sm font-medium text-slate-300">Heartbeat action</span>
          <input
            value={settings.heartbeatAction}
            onChange={(event: ChangeEvent<HTMLInputElement>) => update('heartbeatAction', event.target.value.trim() || 'heartbeat')}
            className="w-full rounded-2xl border border-slate-800 bg-slate-900/80 px-4 py-3 font-mono text-sm text-slate-100 outline-none transition focus:border-sky-500"
          />
        </label>

        <label className="space-y-2">
          <span className="text-sm font-medium text-slate-300">Heartbeat interval ms</span>
          <input
            type="number"
            min={30_000}
            step={10_000}
            value={settings.heartbeatIntervalMs}
            onChange={(event: ChangeEvent<HTMLInputElement>) => update('heartbeatIntervalMs', Number(event.target.value) || 240_000)}
            className="w-full rounded-2xl border border-slate-800 bg-slate-900/80 px-4 py-3 font-mono text-sm text-slate-100 outline-none transition focus:border-sky-500"
          />
        </label>
      </div>

      <div className="mt-5 flex items-center gap-2 rounded-2xl bg-sky-500/10 px-4 py-3 text-sm text-sky-100 ring-1 ring-sky-500/20">
        <Save className="h-4 w-4" />
        Settings are saved locally in this browser and used at the next reconnect.
      </div>
    </section>
  );
};

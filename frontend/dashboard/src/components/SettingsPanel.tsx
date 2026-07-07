import { Save } from 'lucide-react';
import type { DashboardSettings } from '../types/realtime';

interface SettingsPanelProps {
  settings: DashboardSettings;
  enabled: boolean;
  onSettingsChange: (settings: DashboardSettings) => void;
  onEnabledChange: (enabled: boolean) => void;
}

export const SettingsPanel = ({ settings, enabled, onSettingsChange, onEnabledChange }: SettingsPanelProps) => {
  const update = <K extends keyof DashboardSettings>(key: K, value: DashboardSettings[K]) => {
    onSettingsChange({ ...settings, [key]: value });
  };

  return (
    <section className="rounded-3xl border border-slate-800 bg-slate-950/80 p-5 backdrop-blur">
      <div className="mb-4 flex items-center justify-between gap-4">
        <div>
          <h2 className="text-lg font-semibold text-white">Runtime configuration</h2>
          <p className="text-sm text-slate-400">Configure the API Gateway WebSocket endpoint and default topics.</p>
        </div>
        <label className="flex items-center gap-3 text-sm text-slate-300">
          <span>Auto connect</span>
          <input
            type="checkbox"
            checked={enabled}
            onChange={(event) => onEnabledChange(event.target.checked)}
            className="h-5 w-5 rounded border-slate-700 bg-slate-900 text-sky-500 focus:ring-sky-500"
          />
        </label>
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        <label className="space-y-2 lg:col-span-2">
          <span className="text-sm font-medium text-slate-300">WebSocket URL</span>
          <input
            value={settings.wsUrl}
            onChange={(event) => update('wsUrl', event.target.value.trim())}
            placeholder="wss://xxxx.execute-api.us-east-1.amazonaws.com/dev"
            className="w-full rounded-2xl border border-slate-800 bg-slate-900 px-4 py-3 font-mono text-sm text-slate-100 outline-none transition placeholder:text-slate-600 focus:border-sky-500"
          />
        </label>

        <label className="space-y-2">
          <span className="text-sm font-medium text-slate-300">Default topics</span>
          <input
            value={settings.defaultTopics.join(', ')}
            onChange={(event) => update('defaultTopics', event.target.value.split(',').map((topic) => topic.trim()).filter(Boolean))}
            placeholder="global, wiki:frwiki, top_pages"
            className="w-full rounded-2xl border border-slate-800 bg-slate-900 px-4 py-3 text-sm text-slate-100 outline-none transition placeholder:text-slate-600 focus:border-sky-500"
          />
        </label>

        <label className="space-y-2">
          <span className="text-sm font-medium text-slate-300">Heartbeat action</span>
          <input
            value={settings.heartbeatAction}
            onChange={(event) => update('heartbeatAction', event.target.value.trim())}
            placeholder="heartbeat"
            className="w-full rounded-2xl border border-slate-800 bg-slate-900 px-4 py-3 text-sm text-slate-100 outline-none transition placeholder:text-slate-600 focus:border-sky-500"
          />
        </label>

        <label className="space-y-2">
          <span className="text-sm font-medium text-slate-300">Heartbeat interval ms</span>
          <input
            type="number"
            min={30_000}
            value={settings.heartbeatIntervalMs}
            onChange={(event) => update('heartbeatIntervalMs', Number(event.target.value))}
            className="w-full rounded-2xl border border-slate-800 bg-slate-900 px-4 py-3 text-sm text-slate-100 outline-none transition focus:border-sky-500"
          />
        </label>

        <div className="flex items-end">
          <div className="inline-flex items-center gap-2 rounded-2xl bg-slate-900 px-4 py-3 text-xs text-slate-400 ring-1 ring-slate-800">
            <Save className="h-4 w-4" />
            Saved automatically in localStorage
          </div>
        </div>
      </div>
    </section>
  );
};

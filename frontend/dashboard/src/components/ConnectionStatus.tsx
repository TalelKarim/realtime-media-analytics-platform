import { Activity, AlertTriangle, CheckCircle2, Clock, PlugZap, RefreshCw, WifiOff } from 'lucide-react';
import type { ConnectionStatus as ConnectionStatusType } from '../types/realtime';
import { formatTime } from '../lib/format';

interface ConnectionStatusProps {
  status: ConnectionStatusType;
  url: string;
  lastConnectedAt?: string;
  lastMessageAt?: string;
  reconnectAttempt: number;
  error?: string;
  onReconnect: () => void;
  onDisconnect: () => void;
}

const statusMeta: Record<ConnectionStatusType, { label: string; className: string; icon: typeof Activity }> = {
  idle: { label: 'Idle', className: 'bg-slate-800 text-slate-300 ring-slate-700', icon: Clock },
  connecting: { label: 'Connecting', className: 'bg-sky-950 text-sky-200 ring-sky-800', icon: RefreshCw },
  connected: { label: 'Connected', className: 'bg-emerald-950 text-emerald-200 ring-emerald-800', icon: CheckCircle2 },
  reconnecting: { label: 'Reconnecting', className: 'bg-amber-950 text-amber-200 ring-amber-800', icon: RefreshCw },
  disconnected: { label: 'Disconnected', className: 'bg-slate-800 text-slate-300 ring-slate-700', icon: WifiOff },
  error: { label: 'Error', className: 'bg-rose-950 text-rose-200 ring-rose-800', icon: AlertTriangle },
};

export const ConnectionStatus = ({
  status,
  url,
  lastConnectedAt,
  lastMessageAt,
  reconnectAttempt,
  error,
  onReconnect,
  onDisconnect,
}: ConnectionStatusProps) => {
  const meta = statusMeta[status];
  const Icon = meta.icon;

  return (
    <section className="rounded-3xl border border-slate-800 bg-slate-950/80 p-5 shadow-glow backdrop-blur">
      <div className="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
        <div className="space-y-3">
          <div className="flex flex-wrap items-center gap-3">
            <span className={`inline-flex items-center gap-2 rounded-full px-3 py-1 text-sm font-semibold ring-1 ${meta.className}`}>
              <Icon className={status === 'connecting' || status === 'reconnecting' ? 'h-4 w-4 animate-spin' : 'h-4 w-4'} />
              {meta.label}
            </span>
            {reconnectAttempt > 0 && (
              <span className="rounded-full bg-slate-900 px-3 py-1 text-xs text-slate-400 ring-1 ring-slate-800">
                Attempt {reconnectAttempt}
              </span>
            )}
          </div>
          <p className="max-w-4xl break-all font-mono text-xs text-slate-400">{url || 'No WebSocket URL configured'}</p>
          {error && <p className="text-sm text-rose-300">{error}</p>}
        </div>

        <div className="flex flex-col gap-3 sm:flex-row sm:items-center">
          <div className="grid grid-cols-2 gap-3 text-xs text-slate-400 sm:min-w-80">
            <div className="rounded-2xl bg-slate-900/80 p-3 ring-1 ring-slate-800">
              <p className="text-slate-500">Last connected</p>
              <p className="font-medium text-slate-200">{formatTime(lastConnectedAt)}</p>
            </div>
            <div className="rounded-2xl bg-slate-900/80 p-3 ring-1 ring-slate-800">
              <p className="text-slate-500">Last message</p>
              <p className="font-medium text-slate-200">{formatTime(lastMessageAt)}</p>
            </div>
          </div>
          <div className="flex gap-2">
            <button
              type="button"
              onClick={onReconnect}
              className="inline-flex items-center gap-2 rounded-2xl bg-sky-500 px-4 py-2 text-sm font-semibold text-white shadow-lg shadow-sky-500/20 transition hover:bg-sky-400"
            >
              <PlugZap className="h-4 w-4" />
              Reconnect
            </button>
            <button
              type="button"
              onClick={onDisconnect}
              className="rounded-2xl bg-slate-900 px-4 py-2 text-sm font-semibold text-slate-200 ring-1 ring-slate-800 transition hover:bg-slate-800"
            >
              Disconnect
            </button>
          </div>
        </div>
      </div>
    </section>
  );
};

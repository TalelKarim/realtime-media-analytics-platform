import { AlertTriangle, CheckCircle2, Info, XCircle } from 'lucide-react';
import type { EventLogEntry } from '../types/realtime';
import { formatTime } from '../lib/format';

interface EventLogProps {
  entries: EventLogEntry[];
}

const iconByLevel = {
  info: Info,
  success: CheckCircle2,
  warning: AlertTriangle,
  error: XCircle,
};

const classByLevel = {
  info: 'text-sky-300',
  success: 'text-emerald-300',
  warning: 'text-amber-300',
  error: 'text-rose-300',
};

export const EventLog = ({ entries }: EventLogProps) => {
  return (
    <section className="rounded-3xl border border-slate-800 bg-slate-950/80 p-5 backdrop-blur">
      <div className="mb-4">
        <h2 className="text-lg font-semibold text-white">Realtime events</h2>
        <p className="text-sm text-slate-400">Connection events, subscriptions and backend messages.</p>
      </div>
      <div className="max-h-80 space-y-3 overflow-y-auto pr-1">
        {entries.length === 0 && <p className="text-sm text-slate-500">No events yet.</p>}
        {entries.map((entry) => {
          const Icon = iconByLevel[entry.level];
          return (
            <div key={entry.id} className="rounded-2xl bg-slate-900/70 p-3 ring-1 ring-slate-800">
              <div className="flex items-start gap-3">
                <Icon className={`mt-0.5 h-4 w-4 ${classByLevel[entry.level]}`} />
                <div className="min-w-0 flex-1">
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <p className="text-sm font-medium text-slate-200">{entry.message}</p>
                    <span className="text-xs text-slate-500">{formatTime(entry.timestamp)}</span>
                  </div>
                  {entry.details && <p className="mt-1 break-all font-mono text-xs text-slate-500">{entry.details}</p>}
                </div>
              </div>
            </div>
          );
        })}
      </div>
    </section>
  );
};

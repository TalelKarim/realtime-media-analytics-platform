import type { LucideIcon } from 'lucide-react';
import { formatNumber, formatPercent } from '../lib/format';

interface MetricCardProps {
  label: string;
  value: number;
  icon: LucideIcon;
  helper?: string;
  percent?: number;
}

export const MetricCard = ({ label, value, icon: Icon, helper, percent }: MetricCardProps) => {
  return (
    <article className="rounded-3xl border border-slate-800 bg-slate-950/80 p-5 shadow-glow backdrop-blur">
      <div className="flex items-start justify-between">
        <div>
          <p className="text-sm font-medium text-slate-400">{label}</p>
          <p className="mt-2 text-3xl font-bold tracking-tight text-white">{formatNumber(value)}</p>
        </div>
        <div className="rounded-2xl bg-sky-500/10 p-3 text-sky-300 ring-1 ring-sky-500/20">
          <Icon className="h-6 w-6" />
        </div>
      </div>
      <div className="mt-4 flex items-center justify-between text-xs text-slate-500">
        <span>{helper}</span>
        {percent !== undefined && <span className="font-semibold text-slate-300">{formatPercent(percent)}</span>}
      </div>
    </article>
  );
};

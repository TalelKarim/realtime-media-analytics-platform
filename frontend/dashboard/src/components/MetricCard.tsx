import type { LucideIcon } from 'lucide-react';
import { formatNumber, formatPercent } from '../lib/format';

interface MetricCardProps {
  label: string;
  value: number;
  icon: LucideIcon;
  helper?: string;
  percent?: number;
  tone?: 'sky' | 'emerald' | 'violet' | 'amber';
}

const toneClasses = {
  sky: 'bg-sky-500/10 text-sky-300 ring-sky-500/20',
  emerald: 'bg-emerald-500/10 text-emerald-300 ring-emerald-500/20',
  violet: 'bg-violet-500/10 text-violet-300 ring-violet-500/20',
  amber: 'bg-amber-500/10 text-amber-300 ring-amber-500/20',
};

export const MetricCard = ({ label, value, icon: Icon, helper, percent, tone = 'sky' }: MetricCardProps) => {
  return (
    <article className="rounded-3xl border border-slate-800/90 bg-slate-950/85 p-5 shadow-glow backdrop-blur transition hover:border-sky-900/70">
      <div className="flex items-start justify-between gap-4">
        <div>
          <p className="text-sm font-medium text-slate-400">{label}</p>
          <p className="mt-2 text-3xl font-black tracking-tight text-white">{formatNumber(value)}</p>
        </div>
        <div className={`rounded-2xl p-3 ring-1 ${toneClasses[tone]}`}>
          <Icon className="h-6 w-6" />
        </div>
      </div>
      <div className="mt-4 flex min-h-5 items-center justify-between gap-3 text-xs text-slate-500">
        <span className="truncate">{helper}</span>
        {percent !== undefined && <span className="font-semibold text-slate-300">{formatPercent(percent)}</span>}
      </div>
    </article>
  );
};

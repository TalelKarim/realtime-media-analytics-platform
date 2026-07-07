import type { ReactNode } from 'react';

interface ChartCardProps {
  title: string;
  description?: string;
  children: ReactNode;
  className?: string;
  badge?: string;
}

export const ChartCard = ({ title, description, children, className = '', badge }: ChartCardProps) => {
  return (
    <section className={`group rounded-3xl border border-slate-800/90 bg-slate-950/85 p-5 shadow-glow backdrop-blur transition hover:border-sky-900/70 ${className}`}>
      <div className="mb-5 flex items-start justify-between gap-4">
        <div>
          <h2 className="text-lg font-semibold text-white">{title}</h2>
          {description && <p className="mt-1 text-sm leading-6 text-slate-400">{description}</p>}
        </div>
        {badge && <span className="shrink-0 rounded-full bg-sky-500/10 px-3 py-1 text-xs font-medium text-sky-200 ring-1 ring-sky-500/20">{badge}</span>}
      </div>
      {children}
    </section>
  );
};

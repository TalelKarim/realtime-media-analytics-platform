import type { ReactNode } from 'react';

interface ChartCardProps {
  title: string;
  description?: string;
  children: ReactNode;
  className?: string;
}

export const ChartCard = ({ title, description, children, className = '' }: ChartCardProps) => {
  return (
    <section className={`rounded-3xl border border-slate-800 bg-slate-950/80 p-5 shadow-glow backdrop-blur ${className}`}>
      <div className="mb-5">
        <h2 className="text-lg font-semibold text-white">{title}</h2>
        {description && <p className="mt-1 text-sm text-slate-400">{description}</p>}
      </div>
      {children}
    </section>
  );
};

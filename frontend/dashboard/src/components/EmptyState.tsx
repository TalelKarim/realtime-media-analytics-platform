import { RadioTower } from 'lucide-react';

interface EmptyStateProps {
  topic: string;
}

export const EmptyState = ({ topic }: EmptyStateProps) => {
  return (
    <div className="flex min-h-64 flex-col items-center justify-center rounded-3xl border border-dashed border-slate-700 bg-slate-950/40 p-10 text-center">
      <div className="rounded-3xl bg-sky-500/10 p-5 text-sky-300 ring-1 ring-sky-500/20">
        <RadioTower className="h-10 w-10" />
      </div>
      <h2 className="mt-5 text-xl font-semibold text-white">Waiting for live data</h2>
      <p className="mt-2 max-w-xl text-sm text-slate-400">
        The dashboard is subscribed to <span className="font-mono text-slate-200">{topic}</span>. Once the broadcaster sends a <span className="font-mono text-slate-200">stats.update</span> message, charts will update automatically.
      </p>
    </div>
  );
};

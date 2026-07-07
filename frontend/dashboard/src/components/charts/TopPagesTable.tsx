import type { TopPage } from '../../types/realtime';
import { formatNumber } from '../../lib/format';

interface TopPagesTableProps {
  data: TopPage[];
}

export const TopPagesTable = ({ data }: TopPagesTableProps) => {
  if (data.length === 0) {
    return <div className="flex min-h-56 items-center justify-center text-sm text-slate-500">No top pages for this topic yet.</div>;
  }

  return (
    <div className="overflow-hidden rounded-2xl border border-slate-800">
      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-slate-800 text-left text-sm">
          <thead className="bg-slate-900/80 text-xs uppercase tracking-wide text-slate-500">
            <tr>
              <th className="px-4 py-3">Page</th>
              <th className="px-4 py-3">Wiki</th>
              <th className="px-4 py-3">Namespace</th>
              <th className="px-4 py-3 text-right">Events</th>
              <th className="px-4 py-3 text-right">Bot</th>
              <th className="px-4 py-3 text-right">Human</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-800 bg-slate-950/60">
            {data.slice(0, 15).map((page, index) => (
              <tr key={`${page.wiki}-${page.title}-${index}`} className="transition hover:bg-slate-900/80">
                <td className="max-w-xl px-4 py-3">
                  <div className="truncate font-medium text-slate-100" title={page.title}>{page.title}</div>
                </td>
                <td className="px-4 py-3 font-mono text-xs text-slate-400">{page.wiki}</td>
                <td className="px-4 py-3 text-slate-400">{page.namespace ?? '—'}</td>
                <td className="px-4 py-3 text-right font-semibold text-white">{formatNumber(page.count)}</td>
                <td className="px-4 py-3 text-right text-violet-200">{formatNumber(page.botCount)}</td>
                <td className="px-4 py-3 text-right text-emerald-200">{formatNumber(page.humanCount)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};

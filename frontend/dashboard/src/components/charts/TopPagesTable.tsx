import type { TopPage } from '../../types/realtime';
import { formatNumber } from '../../lib/format';

interface TopPagesTableProps {
  pages: TopPage[];
}

export const TopPagesTable = ({ pages }: TopPagesTableProps) => {
  const rows = pages.slice(0, 12);

  if (rows.length === 0) {
    return <p className="py-16 text-center text-sm text-slate-500">No top pages yet.</p>;
  }

  return (
    <div className="overflow-hidden rounded-2xl border border-slate-800">
      <div className="max-h-96 overflow-auto">
        <table className="min-w-full divide-y divide-slate-800 text-left text-sm">
          <thead className="sticky top-0 bg-slate-950 text-xs uppercase tracking-wide text-slate-500">
            <tr>
              <th className="px-4 py-3">#</th>
              <th className="px-4 py-3">Wiki</th>
              <th className="px-4 py-3">Namespace</th>
              <th className="px-4 py-3">Title</th>
              <th className="px-4 py-3 text-right">Events</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-800 bg-slate-950/40 text-slate-300">
            {rows.map((page, index) => (
              <tr key={`${page.wiki}-${page.namespace}-${page.title}-${index}`} className="hover:bg-slate-900/80">
                <td className="px-4 py-3 text-slate-500">{index + 1}</td>
                <td className="px-4 py-3 font-medium text-sky-300">{page.wiki}</td>
                <td className="px-4 py-3">{page.namespace ?? '—'}</td>
                <td className="max-w-xl truncate px-4 py-3" title={page.title}>{page.title}</td>
                <td className="px-4 py-3 text-right font-semibold text-white">{formatNumber(page.count)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};

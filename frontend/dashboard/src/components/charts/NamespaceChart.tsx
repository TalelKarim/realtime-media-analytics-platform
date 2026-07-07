import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import type { ChartPoint } from '../../types/realtime';
import { formatNumber } from '../../lib/format';

interface NamespaceChartProps {
  data: ChartPoint[];
}

const tooltipFormatter = (value: number | string) => [formatNumber(Number(value)), 'events'];
const namespaceLabel = (value: string | number) => String(value);

export const NamespaceChart = ({ data }: NamespaceChartProps) => {
  if (data.length === 0) {
    return <div className="flex h-72 items-center justify-center text-sm text-slate-500">No namespace activity for this topic yet.</div>;
  }

  return (
    <div className="h-72">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={data.slice(0, 10)} margin={{ top: 8, right: 16, left: 8, bottom: 8 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" vertical={false} />
          <XAxis dataKey="name" stroke="#94a3b8" tickFormatter={namespaceLabel} />
          <YAxis stroke="#94a3b8" tickFormatter={(value: number) => formatNumber(value)} />
          <Tooltip
            cursor={{ fill: 'rgba(34, 197, 94, 0.08)' }}
            contentStyle={{ background: '#020617', border: '1px solid #1e293b', borderRadius: '16px', color: '#e2e8f0' }}
            formatter={tooltipFormatter}
            labelFormatter={(label: string | number) => `Namespace ${label}`}
          />
          <Bar dataKey="count" name="Events" fill="#22c55e" radius={[10, 10, 0, 0]} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
};

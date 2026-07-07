import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import type { ChartPoint } from '../../types/realtime';
import { formatNumber } from '../../lib/format';

interface NamespaceChartProps {
  data: ChartPoint[];
}

export const NamespaceChart = ({ data }: NamespaceChartProps) => {
  const chartData = data.slice(0, 12);

  if (chartData.length === 0) {
    return <p className="py-16 text-center text-sm text-slate-500">No namespace activity yet.</p>;
  }

  return (
    <div className="h-72">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={chartData} margin={{ top: 5, right: 20, left: 0, bottom: 5 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" vertical={false} />
          <XAxis dataKey="name" stroke="#94a3b8" />
          <YAxis stroke="#94a3b8" tickFormatter={formatNumber} />
          <Tooltip
            cursor={{ fill: 'rgba(14, 165, 233, 0.08)' }}
            contentStyle={{ background: '#020617', border: '1px solid #1e293b', borderRadius: '16px', color: '#e2e8f0' }}
            formatter={(value) => [formatNumber(Number(value)), 'events']}
            labelFormatter={(label) => `Namespace ${label}`}
          />
          <Bar dataKey="count" radius={[10, 10, 0, 0]} fill="#22c55e" />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
};

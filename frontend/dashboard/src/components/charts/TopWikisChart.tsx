import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import type { ChartPoint } from '../../types/realtime';
import { formatNumber } from '../../lib/format';

interface TopWikisChartProps {
  data: ChartPoint[];
}

export const TopWikisChart = ({ data }: TopWikisChartProps) => {
  const chartData = data.slice(0, 10);

  if (chartData.length === 0) {
    return <p className="py-16 text-center text-sm text-slate-500">No wiki activity yet.</p>;
  }

  return (
    <div className="h-80">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={chartData} layout="vertical" margin={{ top: 5, right: 20, left: 40, bottom: 5 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" horizontal={false} />
          <XAxis type="number" stroke="#94a3b8" tickFormatter={formatNumber} />
          <YAxis type="category" dataKey="name" stroke="#94a3b8" width={110} />
          <Tooltip
            cursor={{ fill: 'rgba(14, 165, 233, 0.08)' }}
            contentStyle={{ background: '#020617', border: '1px solid #1e293b', borderRadius: '16px', color: '#e2e8f0' }}
            formatter={(value) => [formatNumber(Number(value)), 'events']}
          />
          <Bar dataKey="count" radius={[0, 10, 10, 0]} fill="#38bdf8" />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
};

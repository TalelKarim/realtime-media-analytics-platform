import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import type { ChartPoint } from '../../types/realtime';
import { formatNumber } from '../../lib/format';

interface TopWikisChartProps {
  data: ChartPoint[];
}

const tooltipFormatter = (value: number | string) => [formatNumber(Number(value)), 'events'];

export const TopWikisChart = ({ data }: TopWikisChartProps) => {
  if (data.length === 0) {
    return <div className="flex h-72 items-center justify-center text-sm text-slate-500">No wiki activity for this topic yet.</div>;
  }

  const chartData = data.slice(0, 10);

  return (
    <div className="h-72">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={chartData} layout="vertical" margin={{ top: 8, right: 24, left: 24, bottom: 8 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" horizontal={false} />
          <XAxis type="number" stroke="#94a3b8" tickFormatter={(value: number) => formatNumber(value)} />
          <YAxis type="category" dataKey="name" stroke="#94a3b8" width={110} tickLine={false} axisLine={false} />
          <Tooltip
            cursor={{ fill: 'rgba(14, 165, 233, 0.08)' }}
            contentStyle={{ background: '#020617', border: '1px solid #1e293b', borderRadius: '16px', color: '#e2e8f0' }}
            formatter={tooltipFormatter}
          />
          <Bar dataKey="count" name="Events" fill="#38bdf8" radius={[0, 10, 10, 0]} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
};

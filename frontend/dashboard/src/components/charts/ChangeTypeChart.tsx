import { Cell, Pie, PieChart, ResponsiveContainer, Tooltip } from 'recharts';
import type { ChartPoint } from '../../types/realtime';
import { formatNumber } from '../../lib/format';

const COLORS = ['#38bdf8', '#22c55e', '#f59e0b', '#a78bfa', '#f43f5e', '#14b8a6'];

interface ChangeTypeChartProps {
  data: ChartPoint[];
}

export const ChangeTypeChart = ({ data }: ChangeTypeChartProps) => {
  const chartData = data.filter((item) => item.count > 0);

  if (chartData.length === 0) {
    return <p className="py-16 text-center text-sm text-slate-500">No change type activity yet.</p>;
  }

  return (
    <div className="grid gap-4 md:grid-cols-[1fr_180px]">
      <div className="h-72">
        <ResponsiveContainer width="100%" height="100%">
          <PieChart>
            <Pie data={chartData} dataKey="count" nameKey="name" innerRadius={68} outerRadius={105} paddingAngle={3}>
              {chartData.map((entry, index) => (
                <Cell key={entry.name} fill={COLORS[index % COLORS.length]} />
              ))}
            </Pie>
            <Tooltip
              contentStyle={{ background: '#020617', border: '1px solid #1e293b', borderRadius: '16px', color: '#e2e8f0' }}
              formatter={(value) => [formatNumber(Number(value)), 'events']}
            />
          </PieChart>
        </ResponsiveContainer>
      </div>
      <div className="space-y-2 self-center">
        {chartData.map((item, index) => (
          <div key={item.name} className="flex items-center justify-between gap-3 rounded-2xl bg-slate-900/70 px-3 py-2 text-sm ring-1 ring-slate-800">
            <div className="flex items-center gap-2">
              <span className="h-3 w-3 rounded-full" style={{ backgroundColor: COLORS[index % COLORS.length] }} />
              <span className="text-slate-300">{item.name}</span>
            </div>
            <span className="font-semibold text-white">{formatNumber(item.count)}</span>
          </div>
        ))}
      </div>
    </div>
  );
};

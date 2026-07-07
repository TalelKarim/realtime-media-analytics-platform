import { Cell, Pie, PieChart, ResponsiveContainer, Tooltip } from 'recharts';
import type { ChartPoint } from '../../types/realtime';
import { formatNumber } from '../../lib/format';

interface ChangeTypeChartProps {
  data: ChartPoint[];
}

const COLORS = ['#38bdf8', '#22c55e', '#f59e0b', '#a78bfa', '#fb7185', '#14b8a6'];
const tooltipFormatter = (value: number | string) => [formatNumber(Number(value)), 'events'];

export const ChangeTypeChart = ({ data }: ChangeTypeChartProps) => {
  if (data.length === 0) {
    return <div className="flex h-72 items-center justify-center text-sm text-slate-500">No change type activity for this topic yet.</div>;
  }

  const chartData = data.slice(0, 8);

  return (
    <div className="grid min-h-72 gap-4 md:grid-cols-[1fr_220px] md:items-center">
      <div className="h-72">
        <ResponsiveContainer width="100%" height="100%">
          <PieChart>
            <Pie data={chartData} dataKey="count" nameKey="name" innerRadius="58%" outerRadius="84%" paddingAngle={3} stroke="#020617" strokeWidth={3}>
              {chartData.map((entry, index) => (
                <Cell key={entry.name} fill={COLORS[index % COLORS.length]} />
              ))}
            </Pie>
            <Tooltip contentStyle={{ background: '#020617', border: '1px solid #1e293b', borderRadius: '16px', color: '#e2e8f0' }} formatter={tooltipFormatter} />
          </PieChart>
        </ResponsiveContainer>
      </div>

      <div className="space-y-2">
        {chartData.map((entry, index) => (
          <div key={entry.name} className="flex items-center justify-between rounded-2xl bg-slate-900/75 px-4 py-3 text-sm ring-1 ring-slate-800">
            <span className="flex min-w-0 items-center gap-2 text-slate-300">
              <span className="h-2.5 w-2.5 shrink-0 rounded-full" style={{ background: COLORS[index % COLORS.length] }} />
              <span className="truncate">{entry.name}</span>
            </span>
            <span className="font-semibold text-slate-100">{formatNumber(entry.count)}</span>
          </div>
        ))}
      </div>
    </div>
  );
};

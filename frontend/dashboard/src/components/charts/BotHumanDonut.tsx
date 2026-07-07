import { Cell, Pie, PieChart, ResponsiveContainer, Tooltip } from 'recharts';
import { formatNumber, formatPercent } from '../../lib/format';

interface BotHumanDonutProps {
  botCount: number;
  humanCount: number;
}

const COLORS = {
  Bot: '#a78bfa',
  Human: '#22c55e',
};

export const BotHumanDonut = ({ botCount, humanCount }: BotHumanDonutProps) => {
  const total = botCount + humanCount;
  const data = [
    { name: 'Human', count: humanCount },
    { name: 'Bot', count: botCount },
  ].filter((item) => item.count > 0);

  if (total === 0) {
    return <p className="py-16 text-center text-sm text-slate-500">No bot/human distribution yet.</p>;
  }

  return (
    <div className="grid gap-4 md:grid-cols-[1fr_180px]">
      <div className="relative h-72">
        <ResponsiveContainer width="100%" height="100%">
          <PieChart>
            <Pie data={data} dataKey="count" nameKey="name" innerRadius={75} outerRadius={108} paddingAngle={4}>
              {data.map((entry) => (
                <Cell key={entry.name} fill={COLORS[entry.name as keyof typeof COLORS]} />
              ))}
            </Pie>
            <Tooltip
              contentStyle={{ background: '#020617', border: '1px solid #1e293b', borderRadius: '16px', color: '#e2e8f0' }}
              formatter={(value) => [formatNumber(Number(value)), 'events']}
            />
          </PieChart>
        </ResponsiveContainer>
        <div className="pointer-events-none absolute inset-0 flex items-center justify-center">
          <div className="text-center">
            <p className="text-2xl font-bold text-white">{formatNumber(total)}</p>
            <p className="text-xs text-slate-500">events</p>
          </div>
        </div>
      </div>
      <div className="space-y-3 self-center">
        {data.map((item) => (
          <div key={item.name} className="rounded-2xl bg-slate-900/70 p-3 ring-1 ring-slate-800">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                <span className="h-3 w-3 rounded-full" style={{ backgroundColor: COLORS[item.name as keyof typeof COLORS] }} />
                <span className="text-sm text-slate-300">{item.name}</span>
              </div>
              <span className="text-sm font-semibold text-white">{formatPercent((item.count / total) * 100)}</span>
            </div>
            <p className="mt-2 text-2xl font-bold text-white">{formatNumber(item.count)}</p>
          </div>
        ))}
      </div>
    </div>
  );
};

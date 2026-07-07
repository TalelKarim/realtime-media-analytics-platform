import { Cell, Pie, PieChart, ResponsiveContainer, Tooltip } from 'recharts';
import { formatNumber, formatPercent } from '../../lib/format';

interface BotHumanDonutProps {
  botCount: number;
  humanCount: number;
}

const COLORS = {
  Human: '#22c55e',
  Bot: '#a78bfa',
};

const tooltipFormatter = (value: number | string) => [formatNumber(Number(value)), 'events'];

export const BotHumanDonut = ({ botCount, humanCount }: BotHumanDonutProps) => {
  const total = botCount + humanCount;

  if (total === 0) {
    return <div className="flex h-72 items-center justify-center text-sm text-slate-500">No bot/human distribution for this topic yet.</div>;
  }

  const data = [
    { name: 'Human', count: humanCount },
    { name: 'Bot', count: botCount },
  ].filter((item) => item.count > 0);

  return (
    <div className="grid min-h-72 gap-4 md:grid-cols-[1fr_180px] md:items-center">
      <div className="h-72">
        <ResponsiveContainer width="100%" height="100%">
          <PieChart>
            <Pie data={data} dataKey="count" nameKey="name" innerRadius="58%" outerRadius="84%" paddingAngle={3} stroke="#020617" strokeWidth={3}>
              {data.map((entry) => (
                <Cell key={entry.name} fill={COLORS[entry.name as keyof typeof COLORS]} />
              ))}
            </Pie>
            <Tooltip contentStyle={{ background: '#020617', border: '1px solid #1e293b', borderRadius: '16px', color: '#e2e8f0' }} formatter={tooltipFormatter} />
          </PieChart>
        </ResponsiveContainer>
      </div>

      <div className="space-y-3">
        {data.map((entry) => {
          const percent = total > 0 ? (entry.count / total) * 100 : 0;
          return (
            <div key={entry.name} className="rounded-2xl bg-slate-900/75 p-4 ring-1 ring-slate-800">
              <div className="flex items-center justify-between text-sm">
                <span className="flex items-center gap-2 text-slate-300">
                  <span className="h-3 w-3 rounded-full" style={{ background: COLORS[entry.name as keyof typeof COLORS] }} />
                  {entry.name}
                </span>
                <span className="font-semibold text-slate-200">{formatPercent(percent)}</span>
              </div>
              <p className="mt-2 text-2xl font-black text-white">{formatNumber(entry.count)}</p>
            </div>
          );
        })}
      </div>
    </div>
  );
};

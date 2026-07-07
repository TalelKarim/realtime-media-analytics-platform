export const formatNumber = (value: number | null | undefined): string => {
  const safeValue = typeof value === 'number' && Number.isFinite(value) ? value : 0;
  return new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 }).format(safeValue);
};

export const formatPercent = (value: number | null | undefined): string => {
  const safeValue = typeof value === 'number' && Number.isFinite(value) ? value : 0;
  return new Intl.NumberFormat('en-US', { maximumFractionDigits: 1 }).format(safeValue) + '%';
};

export const formatTime = (iso?: string): string => {
  if (!iso) return 'Never';
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return 'Never';
  return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
};

export const shortTopic = (topic: string): string => topic.length > 18 ? `${topic.slice(0, 16)}…` : topic;

import type { ProcedureStats } from '@/lib/types';
import { formatPrice } from '@/lib/utils';

interface StatsSummaryProps {
  stats: ProcedureStats[];
}

export default function StatsSummary({ stats }: StatsSummaryProps) {
  if (stats.length === 0) return null;

  const totalMin = Math.min(...stats.map((s) => s.min_rate));
  const totalMax = Math.max(...stats.map((s) => s.max_rate));
  const totalSavings = stats.reduce((sum, s) => sum + s.potential_savings, 0);
  const totalProviders = Math.max(...stats.map((s) => s.provider_count));

  const cards = [
    { label: 'Lowest Price', value: formatPrice(totalMin), color: 'text-green-600' },
    { label: 'Highest Price', value: formatPrice(totalMax), color: 'text-red-600' },
    { label: 'Potential Savings', value: formatPrice(totalSavings), color: 'text-primary-600' },
    { label: 'Providers', value: totalProviders.toString(), color: 'text-gray-900' },
  ];

  return (
    <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
      {cards.map((card) => (
        <div key={card.label} className="card text-center py-4 px-3">
          <div className={`text-2xl font-bold ${card.color}`}>{card.value}</div>
          <div className="text-xs text-gray-500 mt-1">{card.label}</div>
        </div>
      ))}
    </div>
  );
}

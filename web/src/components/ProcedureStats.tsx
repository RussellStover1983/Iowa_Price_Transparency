import type { ProcedureStatsDetail } from '@/lib/types';
import { formatPrice, formatNumber } from '@/lib/utils';

interface ProcedureStatsProps {
  stats: ProcedureStatsDetail;
}

export default function ProcedureStatsComponent({ stats }: ProcedureStatsProps) {
  const items = [
    { label: 'Minimum', value: formatPrice(stats.min_rate), color: 'text-green-600' },
    { label: '25th Percentile', value: formatPrice(stats.p25_rate), color: 'text-gray-900' },
    { label: 'Median', value: formatPrice(stats.median_rate), color: 'text-primary-600' },
    { label: 'Average', value: formatPrice(stats.avg_rate), color: 'text-gray-900' },
    { label: '75th Percentile', value: formatPrice(stats.p75_rate), color: 'text-gray-900' },
    { label: 'Maximum', value: formatPrice(stats.max_rate), color: 'text-red-600' },
  ];

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
        {items.map((item) => (
          <div key={item.label} className="card text-center py-4">
            <div className={`text-xl font-bold ${item.color}`}>{item.value}</div>
            <div className="text-xs text-gray-500 mt-1">{item.label}</div>
          </div>
        ))}
      </div>

      <div className="card">
        <h3 className="font-medium text-gray-900 mb-3">Coverage</h3>
        <div className="grid grid-cols-3 gap-4 text-center">
          <div>
            <div className="text-lg font-semibold text-gray-900">
              {formatNumber(stats.provider_count)}
            </div>
            <div className="text-xs text-gray-500">Providers</div>
          </div>
          <div>
            <div className="text-lg font-semibold text-gray-900">
              {formatNumber(stats.payer_count)}
            </div>
            <div className="text-xs text-gray-500">Payers</div>
          </div>
          <div>
            <div className="text-lg font-semibold text-gray-900">
              {formatNumber(stats.rate_count)}
            </div>
            <div className="text-xs text-gray-500">Rate Records</div>
          </div>
        </div>
      </div>

      <div className="card bg-primary-50 border-primary-200 text-center">
        <div className="text-sm text-primary-700 mb-1">Potential Savings</div>
        <div className="text-3xl font-bold text-primary-600">
          {formatPrice(stats.potential_savings)}
        </div>
        <div className="text-xs text-primary-600/70 mt-1">
          Difference between highest and lowest negotiated rates
        </div>
      </div>
    </div>
  );
}

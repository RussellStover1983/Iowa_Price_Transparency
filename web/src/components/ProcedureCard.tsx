import type { ProcedureComparison } from '@/lib/types';
import { formatPrice } from '@/lib/utils';
import Link from 'next/link';

interface ProcedureCardProps {
  procedure: ProcedureComparison;
}

export default function ProcedureCard({ procedure }: ProcedureCardProps) {
  const allRates = procedure.providers.flatMap((p) =>
    p.rates.map((r) => r.negotiated_rate)
  );
  const minRate = allRates.length > 0 ? Math.min(...allRates) : 0;
  const maxRate = allRates.length > 0 ? Math.max(...allRates) : 0;

  return (
    <div className="card">
      <div className="flex items-start justify-between gap-4 mb-4">
        <div>
          <div className="flex items-center gap-2 mb-1">
            <Link
              href={`/procedure?code=${procedure.billing_code}`}
              className="px-2 py-0.5 bg-primary-100 text-primary-700 text-xs font-mono font-medium rounded hover:bg-primary-200 transition-colors"
            >
              {procedure.billing_code}
            </Link>
            {procedure.category && (
              <span className="text-xs text-gray-500">{procedure.category}</span>
            )}
          </div>
          <h3 className="font-medium text-gray-900">
            {procedure.description || 'Unknown procedure'}
          </h3>
          {procedure.common_names.length > 0 && (
            <p className="text-sm text-gray-500 mt-0.5">
              {procedure.common_names.join(', ')}
            </p>
          )}
        </div>
        <div className="text-right shrink-0">
          <div className="text-sm text-gray-500">Range</div>
          <div className="font-semibold text-gray-900">
            {formatPrice(minRate)} &ndash; {formatPrice(maxRate)}
          </div>
        </div>
      </div>

      {procedure.providers.length === 0 ? (
        <p className="text-sm text-gray-500 italic">No provider data available</p>
      ) : (
        <div className="overflow-x-auto -mx-6">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-t border-gray-100">
                <th className="text-left font-medium text-gray-500 px-6 py-2">Provider</th>
                <th className="text-left font-medium text-gray-500 px-3 py-2">City</th>
                <th className="text-left font-medium text-gray-500 px-3 py-2">Payer</th>
                <th className="text-right font-medium text-gray-500 px-6 py-2">Rate</th>
              </tr>
            </thead>
            <tbody>
              {procedure.providers.map((provider) =>
                provider.rates.map((rate, rIdx) => (
                  <tr
                    key={`${provider.provider_id}-${rate.payer_id}-${rIdx}`}
                    className="border-t border-gray-50 hover:bg-gray-50 transition-colors"
                  >
                    {rIdx === 0 ? (
                      <>
                        <td
                          className="px-6 py-2 font-medium text-gray-900"
                          rowSpan={provider.rates.length}
                        >
                          <Link
                            href={`/provider?id=${provider.provider_id}`}
                            className="hover:text-primary-600 transition-colors"
                          >
                            {provider.provider_name}
                          </Link>
                        </td>
                        <td
                          className="px-3 py-2 text-gray-600"
                          rowSpan={provider.rates.length}
                        >
                          {provider.city || '—'}
                        </td>
                      </>
                    ) : null}
                    <td className="px-3 py-2 text-gray-600">{rate.payer_name}</td>
                    <td className="px-6 py-2 text-right font-mono">
                      <span
                        className={
                          rate.negotiated_rate === minRate
                            ? 'text-green-600 font-semibold'
                            : rate.negotiated_rate === maxRate
                              ? 'text-red-600'
                              : 'text-gray-900'
                        }
                      >
                        {formatPrice(rate.negotiated_rate)}
                      </span>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

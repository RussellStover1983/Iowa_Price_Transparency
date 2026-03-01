'use client';

import { useState } from 'react';
import type { ProcedureComparison, ProviderPricing, ProviderRate } from '@/lib/types';
import { formatPrice } from '@/lib/utils';
import Link from 'next/link';

interface ProcedureCardProps {
  procedure: ProcedureComparison;
}

function median(values: number[]): number {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 !== 0
    ? sorted[mid]
    : (sorted[mid - 1] + sorted[mid]) / 2;
}

interface AggregatedProvider {
  provider: ProviderPricing;
  medianRate: number;
  minRate: number;
  maxRate: number;
  rateCount: number;
}

function aggregateProviders(providers: ProviderPricing[]): AggregatedProvider[] {
  return providers.map((provider) => {
    const rates = provider.rates.map((r) => r.negotiated_rate);
    return {
      provider,
      medianRate: median(rates),
      minRate: Math.min(...rates),
      maxRate: Math.max(...rates),
      rateCount: rates.length,
    };
  });
}

function groupRatesByPayer(rates: ProviderRate[]): Map<string, ProviderRate[]> {
  const map = new Map<string, ProviderRate[]>();
  for (const r of rates) {
    const key = r.payer_name;
    if (!map.has(key)) map.set(key, []);
    map.get(key)!.push(r);
  }
  return map;
}

function RateDetailRow({ provider }: { provider: ProviderPricing }) {
  const grouped = groupRatesByPayer(provider.rates);

  return (
    <tr>
      <td colSpan={3} className="px-6 py-3 bg-gray-50 border-t border-gray-100">
        <div className="text-xs text-gray-500 mb-2 font-medium">
          {provider.rates.length} rate{provider.rates.length !== 1 ? 's' : ''} from{' '}
          {grouped.size} payer{grouped.size !== 1 ? 's' : ''}
        </div>
        <div className="space-y-3">
          {Array.from(grouped.entries()).map(([payerName, rates]) => (
            <div key={payerName}>
              <div className="text-xs font-semibold text-gray-700 mb-1">{payerName}</div>
              <div className="grid gap-1">
                {rates.map((r, i) => (
                  <div key={i} className="flex items-center justify-between text-xs">
                    <span className="text-gray-500">
                      {r.rate_type || 'rate'}
                      {r.service_setting ? ` \u00b7 ${r.service_setting}` : ''}
                    </span>
                    <span className="font-mono text-gray-900 ml-4">
                      {formatPrice(r.negotiated_rate)}
                    </span>
                  </div>
                ))}
              </div>
            </div>
          ))}
        </div>
      </td>
    </tr>
  );
}

export default function ProcedureCard({ procedure }: ProcedureCardProps) {
  const [expandedProvider, setExpandedProvider] = useState<number | null>(null);
  const aggregated = aggregateProviders(procedure.providers);
  const allMedians = aggregated.map((a) => a.medianRate);
  const globalMin = allMedians.length > 0 ? Math.min(...allMedians) : 0;
  const globalMax = allMedians.length > 0 ? Math.max(...allMedians) : 0;

  const toggleExpand = (providerId: number) => {
    setExpandedProvider((prev) => (prev === providerId ? null : providerId));
  };

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
            {formatPrice(globalMin)} &ndash; {formatPrice(globalMax)}
          </div>
        </div>
      </div>

      {procedure.providers.length === 0 ? (
        <p className="text-sm text-gray-500 italic">No provider data available</p>
      ) : (
        <div className="-mx-6">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-t border-gray-100">
                <th className="text-left font-medium text-gray-500 px-6 py-2">Provider</th>
                <th className="text-left font-medium text-gray-500 px-3 py-2">City</th>
                <th className="text-right font-medium text-gray-500 px-6 py-2">
                  Typical Price
                </th>
              </tr>
            </thead>
            {aggregated.map(({ provider, medianRate, minRate, maxRate, rateCount }) => {
                const isExpanded = expandedProvider === provider.provider_id;
                return (
                  <tbody key={provider.provider_id}>
                    <tr
                      className={`border-t border-gray-50 transition-colors ${
                        rateCount > 1 ? 'cursor-pointer hover:bg-gray-50' : ''
                      } ${isExpanded ? 'bg-gray-50' : ''}`}
                      onClick={rateCount > 1 ? () => toggleExpand(provider.provider_id) : undefined}
                    >
                      <td className="px-6 py-2 font-medium text-gray-900">
                        <Link
                          href={`/provider?id=${provider.provider_id}`}
                          className="hover:text-primary-600 transition-colors"
                          onClick={(e) => e.stopPropagation()}
                        >
                          {provider.provider_name}
                        </Link>
                      </td>
                      <td className="px-3 py-2 text-gray-600">
                        {provider.city || '\u2014'}
                      </td>
                      <td className="px-6 py-2 text-right">
                        <div className="flex items-center justify-end gap-2">
                          <div>
                            <span
                              className={`font-mono ${
                                medianRate === globalMin
                                  ? 'text-green-600 font-semibold'
                                  : medianRate === globalMax
                                    ? 'text-red-600'
                                    : 'text-gray-900'
                              }`}
                            >
                              {formatPrice(medianRate)}
                            </span>
                            {minRate !== maxRate && (
                              <div className="text-[10px] text-gray-400">
                                {formatPrice(minRate)} &ndash; {formatPrice(maxRate)}
                              </div>
                            )}
                          </div>
                          {rateCount > 1 && (
                            <svg
                              className={`w-4 h-4 text-gray-400 transition-transform ${
                                isExpanded ? 'rotate-180' : ''
                              }`}
                              fill="none"
                              viewBox="0 0 24 24"
                              strokeWidth={2}
                              stroke="currentColor"
                            >
                              <path strokeLinecap="round" strokeLinejoin="round" d="M19.5 8.25l-7.5 7.5-7.5-7.5" />
                            </svg>
                          )}
                        </div>
                      </td>
                    </tr>
                    {isExpanded && <RateDetailRow provider={provider} />}
                  </tbody>
                );
              })}
          </table>
        </div>
      )}
    </div>
  );
}

'use client';

import { useState } from 'react';
import type { ProcedureComparison, ProviderPricing, ProviderRate, MedicareRates } from '@/lib/types';
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

function rateRange(rates: number[]): string {
  if (rates.length === 0) return '—';
  const min = Math.min(...rates);
  const max = Math.max(...rates);
  if (min === max) return formatPrice(min);
  return `${formatPrice(min)} – ${formatPrice(max)}`;
}

type RateFilter = 'actionable' | 'all';

interface SplitRates {
  facility: number[];
  physician: number[];
  other: number[];
}

function splitByBillingClass(rates: ProviderRate[], filter: RateFilter): SplitRates {
  const filtered = filter === 'actionable'
    ? rates.filter((r) => r.rate_type !== 'derived')
    : rates;

  const facility: number[] = [];
  const physician: number[] = [];
  const other: number[] = [];

  for (const r of filtered) {
    const setting = (r.service_setting || '').toLowerCase();
    if (['institutional', 'outpatient', 'inpatient'].includes(setting)) {
      facility.push(r.negotiated_rate);
    } else if (['professional', 'ambulatory'].includes(setting)) {
      physician.push(r.negotiated_rate);
    } else {
      other.push(r.negotiated_rate);
    }
  }

  // If filtering removed everything, fall back to all rates
  if (facility.length === 0 && physician.length === 0 && other.length === 0 && filter === 'actionable') {
    return splitByBillingClass(rates, 'all');
  }

  return { facility, physician, other };
}

interface AggregatedProvider {
  provider: ProviderPricing;
  split: SplitRates;
  primaryRate: number; // for sorting: median of facility rates, or physician, or all
  hasFacility: boolean;
  hasPhysician: boolean;
}

function aggregateProviders(providers: ProviderPricing[], filter: RateFilter): AggregatedProvider[] {
  return providers.map((provider) => {
    const split = splitByBillingClass(provider.rates, filter);
    const hasFacility = split.facility.length > 0;
    const hasPhysician = split.physician.length > 0;

    // Primary rate for sorting: prefer facility median, then physician, then other
    let primaryRate = 0;
    if (hasFacility) primaryRate = median(split.facility);
    else if (hasPhysician) primaryRate = median(split.physician);
    else if (split.other.length > 0) primaryRate = median(split.other);

    return { provider, split, primaryRate, hasFacility, hasPhysician };
  }).filter((a) => a.primaryRate > 0); // Drop providers with no rates after filtering
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

function RateDetailRow({ provider, filter }: { provider: ProviderPricing; filter: RateFilter }) {
  const rates = filter === 'actionable'
    ? provider.rates.filter((r) => r.rate_type !== 'derived')
    : provider.rates;

  // Fall back to all if actionable filter removes everything
  const displayRates = rates.length > 0 ? rates : provider.rates;
  const grouped = groupRatesByPayer(displayRates);
  const derivedCount = provider.rates.filter((r) => r.rate_type === 'derived').length;

  return (
    <tr>
      <td colSpan={4} className="px-6 py-3 bg-gray-50 border-t border-gray-100">
        <div className="text-xs text-gray-500 mb-2 font-medium">
          {displayRates.length} rate{displayRates.length !== 1 ? 's' : ''} from{' '}
          {grouped.size} payer{grouped.size !== 1 ? 's' : ''}
          {filter === 'actionable' && derivedCount > 0 && (
            <span className="text-gray-400 ml-1">
              ({derivedCount} derived rate{derivedCount !== 1 ? 's' : ''} hidden)
            </span>
          )}
        </div>
        <div className="space-y-3">
          {Array.from(grouped.entries()).map(([payerName, pRates]) => (
            <div key={payerName}>
              <div className="text-xs font-semibold text-gray-700 mb-1">{payerName}</div>
              <div className="grid gap-1">
                {pRates.map((r, i) => (
                  <div key={i} className="flex items-center justify-between text-xs">
                    <span className="text-gray-500">
                      <span className={
                        ['institutional', 'outpatient', 'inpatient'].includes((r.service_setting || '').toLowerCase())
                          ? 'text-blue-600'
                          : ['professional', 'ambulatory'].includes((r.service_setting || '').toLowerCase())
                            ? 'text-purple-600'
                            : 'text-gray-500'
                      }>
                        {r.service_setting || 'unknown'}
                      </span>
                      {r.rate_type && (
                        <span className="text-gray-400 ml-1">
                          {'\u00b7'} {r.rate_type}
                        </span>
                      )}
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
  const [rateFilter, setRateFilter] = useState<RateFilter>('actionable');

  const aggregated = aggregateProviders(procedure.providers, rateFilter);

  // Determine which columns to show based on data availability
  const anyFacility = aggregated.some((a) => a.hasFacility);
  const anyPhysician = aggregated.some((a) => a.hasPhysician);

  // Global min/max for highlighting
  const facilityMedians = aggregated.filter((a) => a.hasFacility).map((a) => median(a.split.facility));
  const physicianMedians = aggregated.filter((a) => a.hasPhysician).map((a) => median(a.split.physician));
  const facilityMin = facilityMedians.length > 0 ? Math.min(...facilityMedians) : 0;
  const facilityMax = facilityMedians.length > 0 ? Math.max(...facilityMedians) : 0;
  const physicianMin = physicianMedians.length > 0 ? Math.min(...physicianMedians) : 0;
  const physicianMax = physicianMedians.length > 0 ? Math.max(...physicianMedians) : 0;

  const toggleExpand = (providerId: number) => {
    setExpandedProvider((prev) => (prev === providerId ? null : providerId));
  };

  // Count derived rates across all providers
  const totalDerived = procedure.providers.reduce(
    (sum, p) => sum + p.rates.filter((r) => r.rate_type === 'derived').length,
    0
  );
  const totalRates = procedure.providers.reduce((sum, p) => sum + p.rates.length, 0);

  // Determine column count for detail row colSpan
  const colCount = 2 + (anyFacility ? 1 : 0) + (anyPhysician ? 1 : 0) + (!anyFacility && !anyPhysician ? 1 : 0);

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

        {/* Rate filter toggle */}
        {totalDerived > 0 && (
          <div className="shrink-0">
            <button
              onClick={() => setRateFilter((f) => f === 'actionable' ? 'all' : 'actionable')}
              className="text-xs px-2 py-1 rounded border border-gray-200 text-gray-500 hover:bg-gray-50 transition-colors"
            >
              {rateFilter === 'actionable'
                ? `Show derived (${totalDerived})`
                : `Hide derived`}
            </button>
          </div>
        )}
      </div>

      {/* Legend */}
      <div className="flex flex-wrap items-center gap-3 text-[10px] text-gray-400 mb-2">
        {anyFacility && (
          <span className="flex items-center gap-1">
            <span className="w-2 h-2 rounded-full bg-blue-500 inline-block" />
            Facility (institutional)
          </span>
        )}
        {anyPhysician && (
          <span className="flex items-center gap-1">
            <span className="w-2 h-2 rounded-full bg-purple-500 inline-block" />
            Physician (professional)
          </span>
        )}
        <span className="text-gray-300">
          {aggregated.length} provider{aggregated.length !== 1 ? 's' : ''} {'\u00b7'}{' '}
          {rateFilter === 'actionable' ? `${totalRates - totalDerived}` : totalRates} rates
        </span>
      </div>

      {aggregated.length === 0 ? (
        <p className="text-sm text-gray-500 italic">No provider data available</p>
      ) : (
        <div className="-mx-6">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-t border-gray-100">
                <th className="text-left font-medium text-gray-500 px-6 py-2">Provider</th>
                <th className="text-left font-medium text-gray-500 px-3 py-2">City</th>
                {anyFacility && (
                  <th className="text-right font-medium px-4 py-2">
                    <span className="text-blue-600">Facility</span>
                  </th>
                )}
                {anyPhysician && (
                  <th className="text-right font-medium px-4 py-2">
                    <span className="text-purple-600">Physician</span>
                  </th>
                )}
                {!anyFacility && !anyPhysician && (
                  <th className="text-right font-medium text-gray-500 px-6 py-2">Rate</th>
                )}
              </tr>
              {/* Medicare baseline row */}
              {procedure.medicare && (
                <tr className="border-t border-gray-100 bg-amber-50">
                  <td className="px-6 py-1.5 text-xs font-medium text-amber-700" colSpan={2}>
                    Medicare baseline (Iowa, 2025)
                  </td>
                  {anyFacility && (
                    <td className="px-4 py-1.5 text-right text-xs font-mono text-amber-700">
                      {procedure.medicare.opps_rate
                        ? formatPrice(procedure.medicare.opps_rate)
                        : '—'}
                    </td>
                  )}
                  {anyPhysician && (
                    <td className="px-4 py-1.5 text-right text-xs font-mono text-amber-700">
                      {procedure.medicare.facility_rate
                        ? formatPrice(procedure.medicare.facility_rate)
                        : '—'}
                    </td>
                  )}
                  {!anyFacility && !anyPhysician && (
                    <td className="px-6 py-1.5 text-right text-xs font-mono text-amber-700">
                      {procedure.medicare.facility_rate
                        ? formatPrice(procedure.medicare.facility_rate)
                        : '—'}
                    </td>
                  )}
                  <td />
                </tr>
              )}
            </thead>
            {aggregated.map(({ provider, split, hasFacility, hasPhysician }) => {
              const isExpanded = expandedProvider === provider.provider_id;
              const hasDetail = provider.rates.length > 1;
              return (
                <tbody key={provider.provider_id}>
                  <tr
                    className={`border-t border-gray-50 transition-colors ${
                      hasDetail ? 'cursor-pointer hover:bg-gray-50' : ''
                    } ${isExpanded ? 'bg-gray-50' : ''}`}
                    onClick={hasDetail ? () => toggleExpand(provider.provider_id) : undefined}
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
                    {anyFacility && (
                      <td className="px-4 py-2 text-right">
                        {hasFacility ? (
                          <RateCell
                            rates={split.facility}
                            globalMin={facilityMin}
                            globalMax={facilityMax}
                            color="blue"
                            medicareRate={procedure.medicare?.opps_rate ?? undefined}
                          />
                        ) : (
                          <span className="text-gray-300 text-xs">—</span>
                        )}
                      </td>
                    )}
                    {anyPhysician && (
                      <td className="px-4 py-2 text-right">
                        {hasPhysician ? (
                          <RateCell
                            rates={split.physician}
                            globalMin={physicianMin}
                            globalMax={physicianMax}
                            color="purple"
                            medicareRate={procedure.medicare?.facility_rate ?? undefined}
                          />
                        ) : (
                          <span className="text-gray-300 text-xs">—</span>
                        )}
                      </td>
                    )}
                    {!anyFacility && !anyPhysician && (
                      <td className="px-6 py-2 text-right">
                        <RateCell
                          rates={split.other}
                          globalMin={Math.min(...aggregated.map((a) => median(a.split.other)))}
                          globalMax={Math.max(...aggregated.map((a) => median(a.split.other)))}
                          color="gray"
                          medicareRate={procedure.medicare?.facility_rate ?? undefined}
                        />
                      </td>
                    )}
                    {hasDetail && (
                      <td className="pr-3 py-2 w-6">
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
                      </td>
                    )}
                  </tr>
                  {isExpanded && <RateDetailRow provider={provider} filter={rateFilter} />}
                </tbody>
              );
            })}
          </table>
        </div>
      )}
    </div>
  );
}

function RateCell({
  rates,
  globalMin,
  globalMax,
  color,
  medicareRate,
}: {
  rates: number[];
  globalMin: number;
  globalMax: number;
  color: 'blue' | 'purple' | 'gray';
  medicareRate?: number;
}) {
  const med = median(rates);
  const min = Math.min(...rates);
  const max = Math.max(...rates);

  const isLowest = med === globalMin && globalMin !== globalMax;
  const isHighest = med === globalMax && globalMin !== globalMax;

  const colorClass = isLowest
    ? 'text-green-600 font-semibold'
    : isHighest
      ? 'text-red-600'
      : 'text-gray-900';

  const pctMedicare = medicareRate && medicareRate > 0
    ? Math.round((med / medicareRate) * 100)
    : null;

  return (
    <div>
      <span className={`font-mono ${colorClass}`}>
        {formatPrice(med)}
      </span>
      {pctMedicare !== null && (
        <div className={`text-[10px] font-medium ${
          pctMedicare > 300 ? 'text-red-500' : pctMedicare > 200 ? 'text-amber-500' : 'text-green-500'
        }`}>
          {pctMedicare}% of Medicare
        </div>
      )}
      {rates.length > 1 && (
        <div className="text-[10px] text-gray-400">
          {formatPrice(min)} – {formatPrice(max)}
          <span className="ml-1">({rates.length})</span>
        </div>
      )}
    </div>
  );
}

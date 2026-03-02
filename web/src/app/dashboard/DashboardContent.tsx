'use client';

import { useState, useEffect } from 'react';
import { useSearchParams, useRouter } from 'next/navigation';
import {
  getDashboardFacilities,
  getHospitalRates,
  getMarketPosition,
  getPayerScorecard,
  searchCpt,
} from '@/lib/api';
import type {
  DashboardFacility,
  DashboardHospitalRatesResponse,
  DashboardMarketPositionResponse,
  DashboardPayerScorecardResponse,
  DashboardProcedure,
  DashboardPayerGroup,
  MarketFacility,
  ScorecardPayer,
} from '@/lib/types';
import { formatPrice } from '@/lib/utils';
import LoadingSpinner from '@/components/LoadingSpinner';
import InfoTip from '@/components/InfoTip';
import Link from 'next/link';

type Tab = 'hospital' | 'market' | 'scorecard';

export default function DashboardContent() {
  const router = useRouter();
  const searchParams = useSearchParams();

  const tab = (searchParams.get('tab') as Tab) || 'hospital';
  const ccn = searchParams.get('ccn') || '';
  const code = searchParams.get('code') || '';

  const setTab = (t: Tab) => {
    const sp = new URLSearchParams(searchParams.toString());
    sp.set('tab', t);
    router.replace(`/dashboard/?${sp.toString()}`, { scroll: false });
  };

  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      <div className="mb-6">
        <h1 className="text-2xl font-bold text-gray-900">Payer Negotiation Dashboard</h1>
        <p className="text-sm text-gray-500 mt-1">
          Analyze commercial rates vs. Medicare benchmarks across Iowa facilities
        </p>
      </div>

      {/* Tab bar */}
      <div className="flex gap-1 mb-6 border-b border-gray-200">
        {([
          ['hospital', 'My Hospital'],
          ['market', 'Market Position'],
          ['scorecard', 'Payer Scorecard'],
        ] as [Tab, string][]).map(([key, label]) => (
          <button
            key={key}
            onClick={() => setTab(key)}
            className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${
              tab === key
                ? 'border-primary-600 text-primary-700'
                : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
            }`}
          >
            {label}
          </button>
        ))}
      </div>

      {tab === 'hospital' && <HospitalTab initialCcn={ccn} />}
      {tab === 'market' && <MarketTab code={code} />}
      {tab === 'scorecard' && <ScorecardTab initialCcn={ccn} />}
    </div>
  );
}

/* ===== Facility Selector (CCN-based) ===== */
function FacilitySelector({
  selectedCcn,
  onSelect,
  showOnlyWithData,
}: {
  selectedCcn: string;
  onSelect: (ccn: string) => void;
  showOnlyWithData?: boolean;
}) {
  const [facilities, setFacilities] = useState<DashboardFacility[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    getDashboardFacilities()
      .then((res) => {
        const list = showOnlyWithData
          ? res.facilities.filter((f) => f.has_rate_data)
          : res.facilities;
        setFacilities(list);
      })
      .catch(() => {})
      .finally(() => setLoading(false));
  }, [showOnlyWithData]);

  if (loading) return <div className="text-sm text-gray-400">Loading facilities...</div>;

  return (
    <select
      value={selectedCcn}
      onChange={(e) => onSelect(e.target.value)}
      className="block w-full max-w-md px-3 py-2 border border-gray-300 rounded-lg text-sm focus:ring-primary-500 focus:border-primary-500"
    >
      <option value="">Select a hospital...</option>
      {facilities.map((f) => (
        <option key={f.ccn} value={f.ccn}>
          {f.facility_name} ({f.city})
          {f.bed_count ? ` - ${f.bed_count} beds` : ''}
          {!f.has_rate_data ? ' [no rate data]' : ''}
        </option>
      ))}
    </select>
  );
}

/* ===== My Hospital Tab ===== */
function HospitalTab({ initialCcn }: { initialCcn: string }) {
  const [data, setData] = useState<DashboardHospitalRatesResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [selectedCcn, setSelectedCcn] = useState(initialCcn);
  const router = useRouter();
  const searchParams = useSearchParams();

  const handleSelect = (ccn: string) => {
    setSelectedCcn(ccn);
    const sp = new URLSearchParams(searchParams.toString());
    if (ccn) {
      sp.set('ccn', ccn);
    } else {
      sp.delete('ccn');
    }
    sp.set('tab', 'hospital');
    router.replace(`/dashboard/?${sp.toString()}`, { scroll: false });
  };

  useEffect(() => {
    if (!selectedCcn) return;
    setLoading(true);
    getHospitalRates(selectedCcn)
      .then(setData)
      .catch(() => setData(null))
      .finally(() => setLoading(false));
  }, [selectedCcn]);

  return (
    <div>
      <div className="mb-6">
        <label className="block text-sm font-medium text-gray-700 mb-2">Select Hospital</label>
        <FacilitySelector selectedCcn={selectedCcn} onSelect={handleSelect} showOnlyWithData />
      </div>

      {loading && <LoadingSpinner className="py-12" />}

      {!loading && data && data.facility && !data.error && (
        <>
          <div className="card mb-6">
            <h2 className="text-lg font-bold text-gray-900">{data.facility.name}</h2>
            <p className="text-sm text-gray-500">
              {data.facility.city}
              {data.facility.bed_count ? ` \u00b7 ${data.facility.bed_count} beds` : ''}
              {data.facility.hospital_type ? ` \u00b7 ${data.facility.hospital_type}` : ''}
              {' \u00b7 '}
              {data.procedure_count} procedures {'\u00b7'}{' '}
              {data.payer_count} payer{data.payer_count !== 1 ? 's' : ''}
            </p>
            <p className="text-xs text-gray-400 mt-1">CCN: {data.facility.ccn}</p>
          </div>

          {/* Group procedures by category */}
          {Object.entries(
            data.procedures.reduce((acc: Record<string, DashboardProcedure[]>, proc) => {
              const cat = proc.category || 'other';
              if (!acc[cat]) acc[cat] = [];
              acc[cat].push(proc);
              return acc;
            }, {})
          ).map(([category, procs]) => (
            <div key={category} className="mb-6">
              <h3 className="text-sm font-semibold text-gray-500 uppercase tracking-wide mb-3">
                {category.replace('_', ' ')}
              </h3>
              <div className="space-y-2">
                {procs.map((proc) => (
                  <div key={proc.billing_code} className="card py-3">
                    <div className="flex items-start justify-between gap-4 mb-2">
                      <div>
                        <span className="text-xs font-mono text-primary-600 mr-2">{proc.billing_code}</span>
                        <span className="text-sm font-medium text-gray-900">{proc.description}</span>
                      </div>
                      <div className="flex gap-3 shrink-0 items-center">
                        {proc.medicare_opps_rate && (
                          <span className="text-xs text-amber-600">
                            OPPS: {formatPrice(proc.medicare_opps_rate)}
                            <InfoTip text="CY 2025 Outpatient Prospective Payment System national rate. Used as benchmark for institutional/outpatient rates." className="ml-1" />
                          </span>
                        )}
                        {proc.medicare_facility_rate && (
                          <span className="text-xs text-amber-600">
                            MPFS: {formatPrice(proc.medicare_facility_rate)}
                            <InfoTip text="CY 2025 Medicare Physician Fee Schedule rate for Iowa Locality 00. Used as benchmark for professional/ambulatory rates." className="ml-1" />
                          </span>
                        )}
                      </div>
                    </div>
                    <div className="space-y-1">
                      {proc.payer_rates.map((pr: DashboardPayerGroup) => {
                        const rates = pr.rates.map((r) => r.negotiated_rate);
                        const med = rates.sort((a, b) => a - b)[Math.floor(rates.length / 2)];
                        const pctValues = pr.rates.filter((r) => r.pct_medicare).map((r) => r.pct_medicare!);
                        const medPct = pctValues.length > 0
                          ? pctValues.sort((a, b) => a - b)[Math.floor(pctValues.length / 2)]
                          : null;
                        return (
                          <div key={pr.payer_name} className="flex items-center justify-between text-sm">
                            <span className="text-gray-600">{pr.payer_name}</span>
                            <div className="flex items-center gap-3">
                              <span className="font-mono text-gray-900">{formatPrice(med)}</span>
                              {medPct !== null && (
                                <span className={`text-xs font-medium px-1.5 py-0.5 rounded ${
                                  medPct > 300 ? 'bg-red-100 text-red-700'
                                    : medPct > 200 ? 'bg-amber-100 text-amber-700'
                                    : 'bg-green-100 text-green-700'
                                }`}>
                                  {medPct}% of Medicare
                                </span>
                              )}
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          ))}
        </>
      )}

      {!loading && data && data.error && (
        <div className="card text-center text-amber-600 py-8">{data.error}</div>
      )}

      {!loading && !data && selectedCcn && (
        <div className="card text-center text-gray-500 py-8">No data found for this facility.</div>
      )}
    </div>
  );
}

/* ===== Market Position Tab ===== */
function MarketTab({ code: initialCode }: { code: string }) {
  const [data, setData] = useState<DashboardMarketPositionResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [searchQuery, setSearchQuery] = useState(initialCode);
  const [selectedCode, setSelectedCode] = useState(initialCode);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const [searchResults, setSearchResults] = useState<any[]>([]);
  const [showResults, setShowResults] = useState(false);
  const router = useRouter();
  const searchParams = useSearchParams();

  const doSearch = async (q: string) => {
    if (q.length < 2) { setSearchResults([]); return; }
    try {
      const res = await searchCpt(q);
      setSearchResults(res.results);
      setShowResults(true);
    } catch { setSearchResults([]); }
  };

  const selectCode = (code: string) => {
    setSelectedCode(code);
    setShowResults(false);
    const sp = new URLSearchParams(searchParams.toString());
    sp.set('code', code);
    sp.set('tab', 'market');
    router.replace(`/dashboard/?${sp.toString()}`, { scroll: false });
  };

  useEffect(() => {
    if (!selectedCode) return;
    setLoading(true);
    getMarketPosition(selectedCode)
      .then(setData)
      .catch(() => setData(null))
      .finally(() => setLoading(false));
  }, [selectedCode]);

  return (
    <div>
      <div className="mb-6">
        <label className="block text-sm font-medium text-gray-700 mb-2">Search Procedure</label>
        <div className="relative max-w-md">
          <input
            type="text"
            value={searchQuery}
            onChange={(e) => { setSearchQuery(e.target.value); doSearch(e.target.value); }}
            onFocus={() => searchResults.length > 0 && setShowResults(true)}
            placeholder="Type a procedure name or CPT code..."
            className="block w-full px-3 py-2 border border-gray-300 rounded-lg text-sm focus:ring-primary-500 focus:border-primary-500"
          />
          {showResults && searchResults.length > 0 && (
            <div className="absolute z-10 w-full mt-1 bg-white border border-gray-200 rounded-lg shadow-lg max-h-60 overflow-y-auto">
              {searchResults.map((r) => (
                <button
                  key={r.code}
                  onClick={() => { selectCode(r.code); setSearchQuery(r.code + ' - ' + r.description); }}
                  className="w-full text-left px-3 py-2 text-sm hover:bg-gray-50 border-b border-gray-50 last:border-0"
                >
                  <span className="font-mono text-primary-600 mr-2">{r.code}</span>
                  {r.description}
                </button>
              ))}
            </div>
          )}
        </div>
      </div>

      {loading && <LoadingSpinner className="py-12" />}

      {!loading && data && (
        <>
          <div className="card mb-6">
            <div className="flex items-center gap-2 mb-2">
              <span className="px-2 py-0.5 bg-primary-100 text-primary-700 text-xs font-mono font-medium rounded">
                {data.billing_code}
              </span>
              <span className="text-sm text-gray-500">{data.category}</span>
            </div>
            <h2 className="text-lg font-bold text-gray-900">{data.description}</h2>
            <div className="flex flex-wrap gap-4 mt-3 text-sm">
              {data.market_stats && (
                <>
                  <div>
                    <span className="text-gray-500">Market median:</span>{' '}
                    <span className="font-semibold">{formatPrice(data.market_stats.median)}</span>
                  </div>
                  <div>
                    <span className="text-gray-500">Range:</span>{' '}
                    <span className="font-semibold">
                      {formatPrice(data.market_stats.min)} – {formatPrice(data.market_stats.max)}
                    </span>
                  </div>
                </>
              )}
              {data.medicare?.opps_rate && (
                <div>
                  <span className="text-gray-500">Medicare OPPS:</span>{' '}
                  <span className="font-semibold text-amber-600">{formatPrice(data.medicare.opps_rate)}</span>
                </div>
              )}
              {data.medicare?.facility_rate && (
                <div>
                  <span className="text-gray-500">Medicare MPFS:</span>{' '}
                  <span className="font-semibold text-amber-600">{formatPrice(data.medicare.facility_rate)}</span>
                </div>
              )}
            </div>
          </div>

          {/* Facility ranking table */}
          <div className="card">
            <h3 className="font-semibold text-gray-900 mb-3">
              {data.facility_count} Facilities Ranked by Median Rate
            </h3>
            <div className="-mx-6 overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-t border-gray-100">
                    <th className="text-left font-medium text-gray-500 px-6 py-2 w-8">#</th>
                    <th className="text-left font-medium text-gray-500 px-3 py-2">Facility</th>
                    <th className="text-left font-medium text-gray-500 px-3 py-2">City</th>
                    <th className="text-right font-medium text-gray-500 px-3 py-2">Beds</th>
                    <th className="text-right font-medium text-gray-500 px-3 py-2">
                      Median Rate
                      <InfoTip text="Median of all negotiated rates for this facility+procedure from the primary NPI. Less sensitive to outliers than mean." className="ml-1" />
                    </th>
                    <th className="text-right font-medium text-gray-500 px-3 py-2">
                      % Medicare
                      <InfoTip text="Negotiated rate as a percentage of the applicable Medicare reference rate (OPPS for facility, MPFS for professional)." className="ml-1" />
                    </th>
                    <th className="text-right font-medium text-gray-500 px-6 py-2">
                      Percentile
                      <InfoTip text="Facility's rank among all Iowa facilities for this procedure. 0 = lowest rate, 100 = highest rate." className="ml-1" />
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {data.facilities.map((f: MarketFacility, idx: number) => (
                    <tr key={f.ccn} className="border-t border-gray-50 hover:bg-gray-50">
                      <td className="px-6 py-2 text-gray-400 text-xs">{idx + 1}</td>
                      <td className="px-3 py-2 font-medium text-gray-900">
                        <Link
                          href={`/dashboard/?tab=hospital&ccn=${f.ccn}`}
                          className="hover:text-primary-600"
                        >
                          {f.name}
                        </Link>
                        <div className="text-[10px] text-gray-400">{f.hospital_type}</div>
                      </td>
                      <td className="px-3 py-2 text-gray-600">{f.city || '\u2014'}</td>
                      <td className="px-3 py-2 text-right text-gray-500 text-xs">{f.bed_count || '\u2014'}</td>
                      <td className="px-3 py-2 text-right font-mono text-gray-900">
                        {formatPrice(f.median_rate)}
                        {f.min_rate !== f.max_rate && (
                          <div className="text-[10px] text-gray-400">
                            {formatPrice(f.min_rate)} – {formatPrice(f.max_rate)}
                          </div>
                        )}
                      </td>
                      <td className="px-3 py-2 text-right">
                        {f.pct_medicare !== null ? (
                          <span className={`text-xs font-medium px-1.5 py-0.5 rounded ${
                            f.pct_medicare > 300 ? 'bg-red-100 text-red-700'
                              : f.pct_medicare > 200 ? 'bg-amber-100 text-amber-700'
                              : 'bg-green-100 text-green-700'
                          }`}>
                            {f.pct_medicare}%
                          </span>
                        ) : (
                          <span className="text-gray-300">{'\u2014'}</span>
                        )}
                      </td>
                      <td className="px-6 py-2 text-right">
                        <div className="flex items-center justify-end gap-2">
                          <div className="w-16 h-1.5 bg-gray-200 rounded-full overflow-hidden">
                            <div
                              className="h-full bg-primary-500 rounded-full"
                              style={{ width: `${f.percentile}%` }}
                            />
                          </div>
                          <span className="text-xs text-gray-500 w-8 text-right">
                            {f.percentile}
                          </span>
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </>
      )}
    </div>
  );
}

/* ===== Payer Scorecard Tab ===== */
function ScorecardTab({ initialCcn }: { initialCcn: string }) {
  const [data, setData] = useState<DashboardPayerScorecardResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [selectedCcn, setSelectedCcn] = useState(initialCcn);
  const router = useRouter();
  const searchParams = useSearchParams();

  const handleSelect = (ccn: string) => {
    setSelectedCcn(ccn);
    const sp = new URLSearchParams(searchParams.toString());
    if (ccn) {
      sp.set('ccn', ccn);
    } else {
      sp.delete('ccn');
    }
    sp.set('tab', 'scorecard');
    router.replace(`/dashboard/?${sp.toString()}`, { scroll: false });
  };

  useEffect(() => {
    if (!selectedCcn) return;
    setLoading(true);
    getPayerScorecard(selectedCcn)
      .then(setData)
      .catch(() => setData(null))
      .finally(() => setLoading(false));
  }, [selectedCcn]);

  return (
    <div>
      <div className="mb-6">
        <label className="block text-sm font-medium text-gray-700 mb-2">Select Hospital</label>
        <FacilitySelector selectedCcn={selectedCcn} onSelect={handleSelect} showOnlyWithData />
      </div>

      {loading && <LoadingSpinner className="py-12" />}

      {!loading && data && data.facility && !data.error && (
        <>
          <div className="card mb-6">
            <h2 className="text-lg font-bold text-gray-900">{data.facility.name}</h2>
            <p className="text-sm text-gray-500">
              Payer Performance Scorecard {'\u00b7'} {data.payer_count} payer{data.payer_count !== 1 ? 's' : ''}
            </p>
            <p className="text-xs text-gray-400 mt-1">
              {data.facility.city}
              {data.facility.bed_count ? ` \u00b7 ${data.facility.bed_count} beds` : ''}
              {' \u00b7 CCN: '}{data.facility.ccn}
            </p>
          </div>

          <div className="card">
            <div className="-mx-6 overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-t border-gray-100">
                    <th className="text-left font-medium text-gray-500 px-6 py-2">Payer</th>
                    <th className="text-right font-medium text-gray-500 px-3 py-2">Procedures</th>
                    <th className="text-right font-medium text-gray-500 px-3 py-2">Rates</th>
                    <th className="text-right font-medium text-gray-500 px-3 py-2">Avg Rate</th>
                    <th className="text-right font-medium text-gray-500 px-3 py-2">
                      Median % Medicare
                      <InfoTip text="Median of per-procedure rate-to-Medicare ratios. Primary ranking metric — lower values may indicate underpaying contracts." className="ml-1" />
                    </th>
                    <th className="text-right font-medium text-gray-500 px-6 py-2">
                      Avg % Medicare
                      <InfoTip text="Mean of per-procedure rate-to-Medicare ratios. Compare with median to detect skew from outlier procedures." className="ml-1" />
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {data.payers.map((p: ScorecardPayer) => (
                    <tr key={p.payer_id} className="border-t border-gray-50 hover:bg-gray-50">
                      <td className="px-6 py-3 font-medium text-gray-900">{p.payer_name}</td>
                      <td className="px-3 py-3 text-right text-gray-600">{p.procedure_count}</td>
                      <td className="px-3 py-3 text-right text-gray-600">{p.total_rates}</td>
                      <td className="px-3 py-3 text-right font-mono text-gray-900">
                        {formatPrice(p.avg_rate)}
                      </td>
                      <td className="px-3 py-3 text-right">
                        {p.median_pct_medicare !== null ? (
                          <span className={`text-sm font-semibold px-2 py-1 rounded ${
                            p.median_pct_medicare > 300 ? 'bg-red-100 text-red-700'
                              : p.median_pct_medicare > 200 ? 'bg-amber-100 text-amber-700'
                              : 'bg-green-100 text-green-700'
                          }`}>
                            {p.median_pct_medicare}%
                          </span>
                        ) : (
                          <span className="text-gray-300">{'\u2014'}</span>
                        )}
                      </td>
                      <td className="px-6 py-3 text-right">
                        {p.avg_pct_medicare !== null ? (
                          <span className={`text-xs font-medium ${
                            p.avg_pct_medicare > 300 ? 'text-red-600'
                              : p.avg_pct_medicare > 200 ? 'text-amber-600'
                              : 'text-green-600'
                          }`}>
                            {p.avg_pct_medicare}%
                          </span>
                        ) : (
                          <span className="text-gray-300">{'\u2014'}</span>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </>
      )}

      {!loading && data && data.error && (
        <div className="card text-center text-amber-600 py-8">{data.error}</div>
      )}
    </div>
  );
}

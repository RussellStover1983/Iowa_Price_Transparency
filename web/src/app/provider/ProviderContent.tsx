'use client';

import { useEffect, useState } from 'react';
import { useSearchParams } from 'next/navigation';
import Link from 'next/link';
import type { Provider, ProviderProceduresResponse } from '@/lib/types';
import { getProvider, getProviderProcedures } from '@/lib/api';
import { formatPrice } from '@/lib/utils';
import LoadingSpinner from '@/components/LoadingSpinner';
import Pagination from '@/components/Pagination';

const PAGE_SIZE = 50;

export default function ProviderContent() {
  const searchParams = useSearchParams();
  const id = Number(searchParams.get('id'));

  const [provider, setProvider] = useState<Provider | null>(null);
  const [procedures, setProcedures] = useState<ProviderProceduresResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [offset, setOffset] = useState(0);

  useEffect(() => {
    if (!id) {
      setLoading(false);
      return;
    }
    setLoading(true);
    Promise.all([
      getProvider(id),
      getProviderProcedures(id, { limit: PAGE_SIZE, offset }),
    ])
      .then(([prov, procs]) => {
        setProvider(prov);
        setProcedures(procs);
      })
      .catch((err) => setError(err instanceof Error ? err.message : 'Failed to load'))
      .finally(() => setLoading(false));
  }, [id, offset]);

  if (!id) {
    return (
      <div className="max-w-7xl mx-auto px-4 py-12 text-center text-gray-500">
        No provider specified. <Link href="/app/providers" className="text-primary-600 hover:underline">Browse all providers</Link>
      </div>
    );
  }

  if (loading) return <LoadingSpinner className="py-24" />;
  if (error) {
    return (
      <div className="max-w-7xl mx-auto px-4 py-12">
        <div className="card bg-red-50 border-red-200 text-red-700 text-center">{error}</div>
      </div>
    );
  }
  if (!provider) return null;

  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      <nav className="text-sm text-gray-500 mb-6">
        <Link href="/app/providers" className="hover:text-primary-600">
          Providers
        </Link>
        <span className="mx-2">/</span>
        <span className="text-gray-900">{provider.name}</span>
      </nav>

      <div className="card mb-8">
        <h1 className="text-2xl font-bold text-gray-900 mb-3">{provider.name}</h1>
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-2 text-sm text-gray-600">
          {provider.address && <div>Address: {provider.address}</div>}
          {provider.city && (
            <div>
              Location: {provider.city}, {provider.state} {provider.zip_code}
            </div>
          )}
          {provider.county && <div>County: {provider.county}</div>}
          {provider.facility_type && <div>Type: {provider.facility_type}</div>}
          {provider.npi && <div>NPI: {provider.npi}</div>}
        </div>
      </div>

      <h2 className="text-xl font-semibold text-gray-900 mb-4">
        Procedures ({procedures?.total || 0})
      </h2>

      {procedures && procedures.procedures.length > 0 ? (
        <>
          <div className="space-y-4">
            {procedures.procedures.map((proc) => (
              <div key={proc.billing_code} className="card">
                <div className="flex items-start justify-between gap-4 mb-3">
                  <div>
                    <div className="flex items-center gap-2 mb-1">
                      <Link
                        href={`/app/procedure?code=${proc.billing_code}`}
                        className="px-2 py-0.5 bg-primary-100 text-primary-700 text-xs font-mono font-medium rounded hover:bg-primary-200"
                      >
                        {proc.billing_code}
                      </Link>
                      {proc.category && (
                        <span className="text-xs text-gray-500">{proc.category}</span>
                      )}
                    </div>
                    <h3 className="font-medium text-gray-900">
                      {proc.description || 'Unknown procedure'}
                    </h3>
                  </div>
                  <div className="text-right shrink-0">
                    <div className="text-sm text-gray-500">Range</div>
                    <div className="font-semibold">
                      {formatPrice(proc.min_rate)} &ndash; {formatPrice(proc.max_rate)}
                    </div>
                  </div>
                </div>
                <div className="overflow-x-auto -mx-6">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-t border-gray-100">
                        <th className="text-left font-medium text-gray-500 px-6 py-2">Payer</th>
                        <th className="text-right font-medium text-gray-500 px-6 py-2">Rate</th>
                      </tr>
                    </thead>
                    <tbody>
                      {proc.rates.map((rate, i) => (
                        <tr key={i} className="border-t border-gray-50 hover:bg-gray-50">
                          <td className="px-6 py-2 text-gray-700">{rate.payer_name}</td>
                          <td className="px-6 py-2 text-right font-mono">
                            {formatPrice(rate.negotiated_rate)}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            ))}
          </div>
          <Pagination
            total={procedures.total}
            limit={PAGE_SIZE}
            offset={offset}
            onPageChange={setOffset}
          />
        </>
      ) : (
        <div className="card text-center text-gray-500 py-8">
          No procedure data available for this provider.
        </div>
      )}
    </div>
  );
}

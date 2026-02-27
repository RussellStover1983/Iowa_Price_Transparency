'use client';

import { useState, useEffect, useCallback } from 'react';
import { useProviders } from '@/hooks/useProviders';
import ProviderCard from '@/components/ProviderCard';
import Pagination from '@/components/Pagination';
import LoadingSpinner from '@/components/LoadingSpinner';
import EmptyState from '@/components/EmptyState';

const PAGE_SIZE = 24;

export default function ProvidersPage() {
  const { data, loading, error, fetch } = useProviders();
  const [city, setCity] = useState('');
  const [offset, setOffset] = useState(0);

  const loadProviders = useCallback(
    (c: string, o: number) => {
      fetch({ city: c || undefined, limit: PAGE_SIZE, offset: o });
    },
    [fetch]
  );

  useEffect(() => {
    loadProviders(city, offset);
  }, [loadProviders, city, offset]);

  const handleCityChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setCity(e.target.value);
    setOffset(0);
  };

  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      <div className="mb-8">
        <h1 className="text-3xl font-bold text-gray-900 mb-2">Iowa Healthcare Providers</h1>
        <p className="text-gray-600">
          Browse hospitals and healthcare facilities across Iowa with pricing data.
        </p>
      </div>

      <div className="mb-6">
        <input
          type="text"
          value={city}
          onChange={handleCityChange}
          placeholder="Filter by city..."
          className="input max-w-xs"
        />
      </div>

      {loading && <LoadingSpinner className="py-12" />}

      {error && (
        <div className="card bg-red-50 border-red-200 text-red-700 text-center py-6">
          {error}
        </div>
      )}

      {!loading && !error && data && (
        <>
          <p className="text-sm text-gray-500 mb-4">
            Showing {data.count} of {data.total} providers
          </p>
          {data.providers.length === 0 ? (
            <EmptyState title="No providers found" description="Try a different city filter." />
          ) : (
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
              {data.providers.map((p) => (
                <ProviderCard key={p.id} provider={p} />
              ))}
            </div>
          )}
          <Pagination
            total={data.total}
            limit={PAGE_SIZE}
            offset={offset}
            onPageChange={setOffset}
          />
        </>
      )}
    </div>
  );
}

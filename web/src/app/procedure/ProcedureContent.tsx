'use client';

import { useEffect } from 'react';
import { useSearchParams } from 'next/navigation';
import Link from 'next/link';
import { useProcedureStats } from '@/hooks/useProcedureStats';
import ProcedureStatsComponent from '@/components/ProcedureStats';
import LoadingSpinner from '@/components/LoadingSpinner';

export default function ProcedureContent() {
  const searchParams = useSearchParams();
  const code = searchParams.get('code') || '';

  const { data, loading, error, fetch } = useProcedureStats();

  useEffect(() => {
    if (code) fetch(code);
  }, [code, fetch]);

  if (!code) {
    return (
      <div className="max-w-7xl mx-auto px-4 py-12 text-center text-gray-500">
        No procedure code specified. <Link href="/search" className="text-primary-600 hover:underline">Search for procedures</Link>
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
  if (!data) return null;

  return (
    <div className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      <nav className="text-sm text-gray-500 mb-6">
        <Link href="/search" className="hover:text-primary-600">
          Search
        </Link>
        <span className="mx-2">/</span>
        <span className="text-gray-900">{code}</span>
      </nav>

      <div className="mb-8">
        <div className="flex items-center gap-3 mb-2">
          <span className="px-3 py-1 bg-primary-100 text-primary-700 font-mono font-semibold rounded-lg">
            {data.billing_code}
          </span>
          {data.category && (
            <span className="text-sm text-gray-500">{data.category}</span>
          )}
        </div>
        <h1 className="text-2xl font-bold text-gray-900">
          {data.description || `Procedure ${code}`}
        </h1>
      </div>

      <ProcedureStatsComponent stats={data} />

      <div className="mt-8 text-center">
        <Link
          href={`/search?codes=${code}`}
          className="btn-primary"
        >
          Compare prices for this procedure
        </Link>
      </div>
    </div>
  );
}

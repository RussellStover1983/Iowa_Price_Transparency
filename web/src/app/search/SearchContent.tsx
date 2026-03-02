'use client';

import { useState, useEffect, useCallback, useRef } from 'react';
import { useSearchParams, useRouter } from 'next/navigation';
import type { CptSearchResult, Payer } from '@/lib/types';
import { getPayers, searchCpt } from '@/lib/api';
import { useCompare } from '@/hooks/useCompare';
import SearchBar from '@/components/SearchBar';
import SelectedCodes from '@/components/SelectedCodes';
import CompareFilters from '@/components/CompareFilters';
import CompareResults from '@/components/CompareResults';
import ExportButton from '@/components/ExportButton';
import ShareButton from '@/components/ShareButton';
import LoadingSpinner from '@/components/LoadingSpinner';
import EmptyState from '@/components/EmptyState';

export default function SearchContent() {
  const router = useRouter();
  const searchParams = useSearchParams();

  const [selectedCodes, setSelectedCodes] = useState<CptSearchResult[]>([]);
  const [payers, setPayers] = useState<Payer[]>([]);
  const [selectedPayer, setSelectedPayer] = useState('');
  const [selectedCity, setSelectedCity] = useState('');
  const [selectedSort, setSelectedSort] = useState('');
  const [cities, setCities] = useState<string[]>([]);
  const initializedRef = useRef(false);

  const { data, loading, error, compare } = useCompare();

  // Load payers on mount
  useEffect(() => {
    getPayers()
      .then(setPayers)
      .catch(() => {});
  }, []);

  // Extract unique cities from compare results
  useEffect(() => {
    if (!data) return;
    const citySet = new Set<string>();
    for (const proc of data.procedures) {
      for (const provider of proc.providers) {
        if (provider.city) citySet.add(provider.city);
      }
    }
    setCities(Array.from(citySet).sort());
  }, [data]);

  // Sync URL params to state on mount (for shareable URLs)
  useEffect(() => {
    if (initializedRef.current) return;
    initializedRef.current = true;

    const codesParam = searchParams.get('codes');
    const payerParam = searchParams.get('payer') || '';
    const cityParam = searchParams.get('city') || '';
    const sortParam = searchParams.get('sort') || '';

    setSelectedPayer(payerParam);
    setSelectedCity(cityParam);
    setSelectedSort(sortParam);

    if (codesParam) {
      const codeList = codesParam.split(',').filter(Boolean);
      // Fetch CPT info for each code to populate chips
      Promise.all(
        codeList.map(async (code) => {
          try {
            const res = await searchCpt(code);
            const exact = res.results.find((r) => r.code === code);
            return exact || { code, description: code, category: null, common_names: [], rank: 0 };
          } catch {
            return { code, description: code, category: null, common_names: [], rank: 0 } as CptSearchResult;
          }
        })
      ).then((results) => {
        setSelectedCodes(results);
        compare({
          codes: codeList,
          payer: payerParam || undefined,
          city: cityParam || undefined,
          sort: sortParam || undefined,
        });
      });
    }
  }, [searchParams, compare]);

  // Update URL when state changes
  const updateUrl = useCallback(
    (codes: string[], payer: string, city: string, sort: string) => {
      const sp = new URLSearchParams();
      if (codes.length > 0) sp.set('codes', codes.join(','));
      if (payer) sp.set('payer', payer);
      if (city) sp.set('city', city);
      if (sort) sp.set('sort', sort);
      const qs = sp.toString();
      router.replace(`/search/${qs ? '?' + qs : ''}`, { scroll: false });
    },
    [router]
  );

  // Run comparison whenever codes or filters change
  const doCompare = useCallback(
    (codes: CptSearchResult[], payer: string, city: string, sort: string) => {
      const codeStrs = codes.map((c) => c.code);
      updateUrl(codeStrs, payer, city, sort);
      if (codes.length === 0) return;
      compare({
        codes: codeStrs,
        payer: payer || undefined,
        city: city || undefined,
        sort: sort || undefined,
      });
    },
    [compare, updateUrl]
  );

  const handleSelect = (result: CptSearchResult) => {
    if (selectedCodes.some((c) => c.code === result.code)) return;
    if (selectedCodes.length >= 10) return;
    const next = [...selectedCodes, result];
    setSelectedCodes(next);
    doCompare(next, selectedPayer, selectedCity, selectedSort);
  };

  const handleRemove = (code: string) => {
    const next = selectedCodes.filter((c) => c.code !== code);
    setSelectedCodes(next);
    doCompare(next, selectedPayer, selectedCity, selectedSort);
  };

  const handlePayerChange = (payer: string) => {
    setSelectedPayer(payer);
    doCompare(selectedCodes, payer, selectedCity, selectedSort);
  };

  const handleCityChange = (city: string) => {
    setSelectedCity(city);
    doCompare(selectedCodes, selectedPayer, city, selectedSort);
  };

  const handleSortChange = (sort: string) => {
    setSelectedSort(sort);
    doCompare(selectedCodes, selectedPayer, selectedCity, sort);
  };

  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      {/* Hero section */}
      <div className="text-center mb-8">
        <h1 className="text-3xl font-bold text-gray-900 mb-2">
          Compare Negotiated Rates
        </h1>
        <p className="text-gray-600 max-w-2xl mx-auto">
          Search for procedures and compare negotiated rates across Iowa
          facilities and payers, with Medicare benchmarks.
        </p>
      </div>

      {/* Search */}
      <div className="max-w-2xl mx-auto mb-6">
        <SearchBar
          onSelect={handleSelect}
          disabled={selectedCodes.length >= 10}
        />
        {selectedCodes.length >= 10 && (
          <p className="text-xs text-amber-600 mt-1">Maximum 10 procedures selected</p>
        )}
      </div>

      {/* Selected codes */}
      {selectedCodes.length > 0 && (
        <div className="mb-6">
          <SelectedCodes codes={selectedCodes} onRemove={handleRemove} />
        </div>
      )}

      {/* Filters + actions */}
      {selectedCodes.length > 0 && (
        <div className="flex flex-wrap items-center justify-between gap-4 mb-6">
          <CompareFilters
            payers={payers}
            selectedPayer={selectedPayer}
            selectedCity={selectedCity}
            selectedSort={selectedSort}
            cities={cities}
            onPayerChange={handlePayerChange}
            onCityChange={handleCityChange}
            onSortChange={handleSortChange}
          />
          <div className="flex gap-2">
            <ExportButton
              codes={selectedCodes.map((c) => c.code)}
              payer={selectedPayer || undefined}
              city={selectedCity || undefined}
            />
            <ShareButton />
          </div>
        </div>
      )}

      {/* Results */}
      {loading && <LoadingSpinner className="py-12" />}

      {error && (
        <div className="card bg-red-50 border-red-200 text-red-700 text-center py-6">
          {error}
        </div>
      )}

      {!loading && !error && data && <CompareResults data={data} />}

      {!loading && !error && !data && selectedCodes.length === 0 && (
        <EmptyState
          title="Start by searching for a procedure"
          description='Type a procedure name like "knee replacement" or a CPT code like "27447" in the search bar above.'
          icon={
            <svg className="w-16 h-16" fill="none" viewBox="0 0 24 24" strokeWidth={1} stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" d="M21 21l-5.197-5.197m0 0A7.5 7.5 0 105.196 5.196a7.5 7.5 0 0010.607 10.607z" />
            </svg>
          }
        />
      )}
    </div>
  );
}

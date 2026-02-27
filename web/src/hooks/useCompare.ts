'use client';

import { useState, useCallback } from 'react';
import type { CompareResponse } from '@/lib/types';
import { comparePrices } from '@/lib/api';

export function useCompare() {
  const [data, setData] = useState<CompareResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const compare = useCallback(
    async (params: {
      codes: string[];
      payer?: string;
      city?: string;
      county?: string;
      sort?: string;
    }) => {
      if (params.codes.length === 0) {
        setData(null);
        return;
      }
      setLoading(true);
      setError(null);
      try {
        const result = await comparePrices(params);
        setData(result);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to load comparison');
        setData(null);
      } finally {
        setLoading(false);
      }
    },
    []
  );

  const clear = useCallback(() => {
    setData(null);
    setError(null);
  }, []);

  return { data, loading, error, compare, clear };
}

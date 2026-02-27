'use client';

import { useState, useCallback } from 'react';
import type { PaginatedProvidersResponse } from '@/lib/types';
import { getProviders } from '@/lib/api';

export function useProviders() {
  const [data, setData] = useState<PaginatedProvidersResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetch = useCallback(
    async (params?: { city?: string; county?: string; limit?: number; offset?: number }) => {
      setLoading(true);
      setError(null);
      try {
        const result = await getProviders(params);
        setData(result);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to load providers');
      } finally {
        setLoading(false);
      }
    },
    []
  );

  return { data, loading, error, fetch };
}

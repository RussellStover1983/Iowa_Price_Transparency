'use client';

import { useState, useCallback } from 'react';
import type { ProcedureStatsDetail } from '@/lib/types';
import { getProcedureStats } from '@/lib/api';

export function useProcedureStats() {
  const [data, setData] = useState<ProcedureStatsDetail | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetch = useCallback(async (code: string) => {
    setLoading(true);
    setError(null);
    try {
      const result = await getProcedureStats(code);
      setData(result);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load procedure stats');
    } finally {
      setLoading(false);
    }
  }, []);

  return { data, loading, error, fetch };
}

import type {
  CptSearchResponse,
  CompareResponse,
  Payer,
  PaginatedProvidersResponse,
  Provider,
  ProviderProceduresResponse,
  ProcedureStatsDetail,
  CoverageStats,
  HealthResponse,
} from './types';

const API_BASE = typeof window !== 'undefined' ? window.location.origin : '';

async function fetchApi<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`);
  if (!res.ok) {
    const body = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(body.detail || `API error: ${res.status}`);
  }
  return res.json();
}

export async function searchCpt(query: string): Promise<CptSearchResponse> {
  return fetchApi(`/v1/cpt/search?q=${encodeURIComponent(query)}`);
}

export async function comparePrices(params: {
  codes: string[];
  payer?: string;
  city?: string;
  county?: string;
  sort?: string;
}): Promise<CompareResponse> {
  const sp = new URLSearchParams();
  sp.set('codes', params.codes.join(','));
  if (params.payer) sp.set('payer', params.payer);
  if (params.city) sp.set('city', params.city);
  if (params.county) sp.set('county', params.county);
  if (params.sort) sp.set('sort', params.sort);
  return fetchApi(`/v1/compare?${sp.toString()}`);
}

export async function getPayers(): Promise<Payer[]> {
  const res = await fetch(`${API_BASE}/v1/payers`);
  if (!res.ok) throw new Error('Failed to fetch payers');
  return res.json();
}

export async function getProviders(params?: {
  city?: string;
  county?: string;
  limit?: number;
  offset?: number;
}): Promise<PaginatedProvidersResponse> {
  const sp = new URLSearchParams();
  if (params?.city) sp.set('city', params.city);
  if (params?.county) sp.set('county', params.county);
  if (params?.limit) sp.set('limit', params.limit.toString());
  if (params?.offset) sp.set('offset', params.offset.toString());
  const qs = sp.toString();
  return fetchApi(`/v1/providers${qs ? '?' + qs : ''}`);
}

export async function getProvider(id: number): Promise<Provider> {
  return fetchApi(`/v1/providers/${id}`);
}

export async function getProviderProcedures(
  id: number,
  params?: { limit?: number; offset?: number }
): Promise<ProviderProceduresResponse> {
  const sp = new URLSearchParams();
  if (params?.limit) sp.set('limit', params.limit.toString());
  if (params?.offset) sp.set('offset', params.offset.toString());
  const qs = sp.toString();
  return fetchApi(`/v1/providers/${id}/procedures${qs ? '?' + qs : ''}`);
}

export async function getProcedureStats(code: string): Promise<ProcedureStatsDetail> {
  return fetchApi(`/v1/procedures/${code}/stats`);
}

export async function getCoverageStats(): Promise<CoverageStats> {
  return fetchApi(`/v1/admin/stats`);
}

export async function getHealth(): Promise<HealthResponse> {
  return fetchApi(`/health`);
}

// Dashboard APIs
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export async function getHospitalRates(providerId: number): Promise<any> {
  return fetchApi(`/v1/dashboard/hospital-rates?provider_id=${providerId}`);
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export async function getMarketPosition(billingCode: string, payer?: string, serviceSetting?: string): Promise<any> {
  const sp = new URLSearchParams();
  sp.set('billing_code', billingCode);
  if (payer) sp.set('payer', payer);
  if (serviceSetting) sp.set('service_setting', serviceSetting);
  return fetchApi(`/v1/dashboard/market-position?${sp.toString()}`);
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export async function getPayerScorecard(providerId: number): Promise<any> {
  return fetchApi(`/v1/dashboard/payer-scorecard?provider_id=${providerId}`);
}

export function getExportUrl(params: {
  codes: string[];
  payer?: string;
  city?: string;
  county?: string;
}): string {
  const sp = new URLSearchParams();
  sp.set('codes', params.codes.join(','));
  sp.set('format', 'csv');
  if (params.payer) sp.set('payer', params.payer);
  if (params.city) sp.set('city', params.city);
  if (params.county) sp.set('county', params.county);
  return `${API_BASE}/v1/export?${sp.toString()}`;
}

/* TypeScript interfaces mirroring db/models.py */

export interface CptSearchResult {
  code: string;
  description: string;
  category: string | null;
  common_names: string[];
  rank: number;
}

export interface CptSearchResponse {
  query: string;
  count: number;
  results: CptSearchResult[];
  disambiguation_used: boolean;
}

export interface ProviderRate {
  payer_id: number;
  payer_name: string;
  negotiated_rate: number;
  rate_type: string | null;
  service_setting: string | null;
}

export interface ProviderPricing {
  provider_id: number;
  provider_name: string;
  city: string | null;
  county: string | null;
  rates: ProviderRate[];
  min_rate: number;
  max_rate: number;
}

export interface MedicareRates {
  facility_rate: number | null;
  professional_rate: number | null;
  opps_rate: number | null;
}

export interface ProcedureComparison {
  billing_code: string;
  description: string | null;
  category: string | null;
  common_names: string[];
  providers: ProviderPricing[];
  provider_count: number;
  medicare: MedicareRates | null;
}

export interface ProcedureStats {
  billing_code: string;
  description: string | null;
  min_rate: number;
  max_rate: number;
  median_rate: number;
  avg_rate: number;
  rate_count: number;
  provider_count: number;
  potential_savings: number;
}

export interface CompareResponse {
  codes_requested: string[];
  procedures: ProcedureComparison[];
  total_providers: number;
  stats: ProcedureStats[];
}

export interface Payer {
  id: number;
  name: string;
  short_name: string;
}

export interface ProviderSummary {
  id: number;
  name: string;
  city: string | null;
  county: string | null;
  facility_type: string | null;
  zip_code: string | null;
  procedure_count: number;
  payer_count: number;
}

export interface PaginatedProvidersResponse {
  count: number;
  providers: ProviderSummary[];
  total: number;
  limit: number;
  offset: number;
}

export interface Provider {
  id: number;
  npi: string | null;
  tin: string | null;
  name: string;
  facility_type: string | null;
  address: string | null;
  city: string | null;
  state: string;
  zip_code: string | null;
  county: string | null;
  active: boolean;
}

export interface ProviderProcedureRate {
  payer_id: number;
  payer_name: string;
  negotiated_rate: number;
  rate_type: string | null;
  service_setting: string | null;
}

export interface ProviderProcedure {
  billing_code: string;
  description: string | null;
  category: string | null;
  rates: ProviderProcedureRate[];
  min_rate: number;
  max_rate: number;
  avg_rate: number;
  payer_count: number;
}

export interface ProviderProceduresResponse {
  provider_id: number;
  provider_name: string;
  procedures: ProviderProcedure[];
  total: number;
  limit: number;
  offset: number;
}

export interface ProcedureStatsDetail {
  billing_code: string;
  description: string | null;
  category: string | null;
  min_rate: number;
  max_rate: number;
  median_rate: number;
  avg_rate: number;
  p25_rate: number;
  p75_rate: number;
  rate_count: number;
  provider_count: number;
  payer_count: number;
  potential_savings: number;
}

export interface CoverageStats {
  total_providers: number;
  total_payers: number;
  total_procedures: number;
  total_rates: number;
  last_updated: string | null;
  db_size_bytes: number;
}

export interface HealthResponse {
  status: string;
  database: string;
}

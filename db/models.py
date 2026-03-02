"""Pydantic response models for the API."""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class HealthResponse(BaseModel):
    status: str
    database: str


class Payer(BaseModel):
    id: int
    name: str
    short_name: str
    toc_url: Optional[str] = None
    state_filter: str = "IA"
    active: bool = True
    last_crawled: Optional[str] = None
    notes: Optional[str] = None


class Provider(BaseModel):
    id: int
    npi: Optional[str] = None
    tin: Optional[str] = None
    name: str
    facility_type: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    state: str = "IA"
    zip_code: Optional[str] = None
    county: Optional[str] = None
    active: bool = True


class CptCode(BaseModel):
    code: str
    description: str
    category: Optional[str] = None
    common_names: Optional[list[str]] = None


class NormalizedRate(BaseModel):
    id: int
    payer_id: int
    provider_id: Optional[int] = None
    billing_code: str
    billing_code_type: str
    description: Optional[str] = None
    negotiated_rate: float
    rate_type: Optional[str] = None
    service_setting: Optional[str] = None
    mrf_file_id: Optional[int] = None


# --- Phase 1 response models ---


class CptSearchResult(BaseModel):
    code: str
    description: str
    category: Optional[str] = None
    common_names: list[str] = []
    rank: float = 0.0


class CptSearchResponse(BaseModel):
    query: str
    count: int
    results: list[CptSearchResult]
    disambiguation_used: bool = False


class ProviderRate(BaseModel):
    payer_id: int
    payer_name: str
    negotiated_rate: float
    rate_type: Optional[str] = None
    service_setting: Optional[str] = None


class ProviderPricing(BaseModel):
    provider_id: int
    provider_name: str
    city: Optional[str] = None
    county: Optional[str] = None
    rates: list[ProviderRate]
    min_rate: float
    max_rate: float


class MedicareRates(BaseModel):
    facility_rate: Optional[float] = None
    professional_rate: Optional[float] = None
    opps_rate: Optional[float] = None


class ProcedureComparison(BaseModel):
    billing_code: str
    description: Optional[str] = None
    category: Optional[str] = None
    common_names: list[str] = []
    providers: list[ProviderPricing]
    provider_count: int
    medicare: Optional[MedicareRates] = None


class ProcedureStats(BaseModel):
    billing_code: str
    description: Optional[str] = None
    min_rate: float
    max_rate: float
    median_rate: float
    avg_rate: float
    rate_count: int
    provider_count: int
    potential_savings: float


class CompareResponse(BaseModel):
    codes_requested: list[str]
    procedures: list[ProcedureComparison]
    total_providers: int
    stats: list[ProcedureStats] = []


class ProviderSummary(BaseModel):
    id: int
    name: str
    city: Optional[str] = None
    county: Optional[str] = None
    facility_type: Optional[str] = None
    zip_code: Optional[str] = None
    procedure_count: int = 0
    payer_count: int = 0


class ProvidersResponse(BaseModel):
    count: int
    providers: list[ProviderSummary]


class PaginatedProvidersResponse(ProvidersResponse):
    total: int = 0
    limit: int = 50
    offset: int = 0


# --- Phase 5 response models ---


class CoverageStats(BaseModel):
    total_providers: int
    total_payers: int
    total_procedures: int
    total_rates: int
    last_updated: Optional[str] = None
    db_size_bytes: int = 0


class ProviderProcedureRate(BaseModel):
    payer_id: int
    payer_name: str
    negotiated_rate: float
    rate_type: Optional[str] = None
    service_setting: Optional[str] = None


class ProviderProcedure(BaseModel):
    billing_code: str
    description: Optional[str] = None
    category: Optional[str] = None
    rates: list[ProviderProcedureRate]
    min_rate: float
    max_rate: float
    avg_rate: float
    payer_count: int


class ProviderProceduresResponse(BaseModel):
    provider_id: int
    provider_name: str
    procedures: list[ProviderProcedure]
    total: int = 0
    limit: int = 50
    offset: int = 0


class ProcedureStatsDetail(BaseModel):
    billing_code: str
    description: Optional[str] = None
    category: Optional[str] = None
    min_rate: float
    max_rate: float
    median_rate: float
    avg_rate: float
    p25_rate: float
    p75_rate: float
    rate_count: int
    provider_count: int
    payer_count: int
    potential_savings: float

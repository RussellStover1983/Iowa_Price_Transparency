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


class ProcedureComparison(BaseModel):
    billing_code: str
    description: Optional[str] = None
    category: Optional[str] = None
    providers: list[ProviderPricing]
    provider_count: int


class CompareResponse(BaseModel):
    codes_requested: list[str]
    procedures: list[ProcedureComparison]
    total_providers: int

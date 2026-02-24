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
    common_names: Optional[str] = None


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

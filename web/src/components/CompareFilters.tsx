'use client';

import type { Payer } from '@/lib/types';

interface CompareFiltersProps {
  payers: Payer[];
  selectedPayer: string;
  selectedCity: string;
  selectedSort: string;
  cities: string[];
  onPayerChange: (payer: string) => void;
  onCityChange: (city: string) => void;
  onSortChange: (sort: string) => void;
}

export default function CompareFilters({
  payers,
  selectedPayer,
  selectedCity,
  selectedSort,
  cities,
  onPayerChange,
  onCityChange,
  onSortChange,
}: CompareFiltersProps) {
  return (
    <div className="flex flex-wrap gap-3">
      <select
        value={selectedPayer}
        onChange={(e) => onPayerChange(e.target.value)}
        className="select text-sm"
      >
        <option value="">All Payers</option>
        {payers.map((p) => (
          <option key={p.short_name} value={p.short_name}>
            {p.name}
          </option>
        ))}
      </select>

      <select
        value={selectedCity}
        onChange={(e) => onCityChange(e.target.value)}
        className="select text-sm"
      >
        <option value="">All Cities</option>
        {cities.map((c) => (
          <option key={c} value={c}>
            {c}
          </option>
        ))}
      </select>

      <select
        value={selectedSort}
        onChange={(e) => onSortChange(e.target.value)}
        className="select text-sm"
      >
        <option value="">Default Sort</option>
        <option value="price_asc">Price: Low to High</option>
        <option value="price_desc">Price: High to Low</option>
      </select>
    </div>
  );
}

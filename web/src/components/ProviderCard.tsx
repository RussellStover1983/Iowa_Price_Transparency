import Link from 'next/link';
import type { ProviderSummary } from '@/lib/types';
import { formatNumber } from '@/lib/utils';

interface ProviderCardProps {
  provider: ProviderSummary;
}

export default function ProviderCard({ provider }: ProviderCardProps) {
  return (
    <Link
      href={`/provider?id=${provider.id}`}
      className="card hover:shadow-md hover:border-primary-200 transition-all group"
    >
      <h3 className="font-semibold text-gray-900 group-hover:text-primary-600 transition-colors mb-2">
        {provider.name}
      </h3>
      <div className="space-y-1 text-sm text-gray-600">
        {provider.city && (
          <div className="flex items-center gap-1.5">
            <svg className="w-4 h-4 text-gray-400 shrink-0" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" d="M15 10.5a3 3 0 11-6 0 3 3 0 016 0z" />
              <path strokeLinecap="round" strokeLinejoin="round" d="M19.5 10.5c0 7.142-7.5 11.25-7.5 11.25S4.5 17.642 4.5 10.5a7.5 7.5 0 1115 0z" />
            </svg>
            {provider.city}{provider.county ? `, ${provider.county} County` : ''}
            {provider.zip_code ? ` ${provider.zip_code}` : ''}
          </div>
        )}
        {provider.facility_type && (
          <div className="text-xs text-gray-500">{provider.facility_type}</div>
        )}
      </div>
      <div className="flex gap-4 mt-3 pt-3 border-t border-gray-100 text-xs">
        <div>
          <span className="font-semibold text-gray-900">{formatNumber(provider.procedure_count)}</span>{' '}
          <span className="text-gray-500">procedures</span>
        </div>
        <div>
          <span className="font-semibold text-gray-900">{formatNumber(provider.payer_count)}</span>{' '}
          <span className="text-gray-500">payers</span>
        </div>
      </div>
    </Link>
  );
}

'use client';

import { getExportUrl } from '@/lib/api';

interface ExportButtonProps {
  codes: string[];
  payer?: string;
  city?: string;
}

export default function ExportButton({ codes, payer, city }: ExportButtonProps) {
  if (codes.length === 0) return null;

  const handleExport = () => {
    const url = getExportUrl({ codes, payer, city });
    window.open(url, '_blank');
  };

  return (
    <button onClick={handleExport} className="btn-secondary text-sm">
      <svg className="w-4 h-4 mr-1.5" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" d="M3 16.5v2.25A2.25 2.25 0 005.25 21h13.5A2.25 2.25 0 0021 18.75V16.5M16.5 12L12 16.5m0 0L7.5 12m4.5 4.5V3" />
      </svg>
      Export CSV
    </button>
  );
}

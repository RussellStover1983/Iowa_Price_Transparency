'use client';

import { useEffect, useState } from 'react';
import type { CoverageStats } from '@/lib/types';
import { getCoverageStats } from '@/lib/api';
import { formatNumber } from '@/lib/utils';

export default function AboutPage() {
  const [stats, setStats] = useState<CoverageStats | null>(null);

  useEffect(() => {
    getCoverageStats().then(setStats).catch(() => {});
  }, []);

  return (
    <div className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      <h1 className="text-3xl font-bold text-gray-900 mb-6">About This Tool</h1>

      <div className="space-y-6">
        <div className="card">
          <h2 className="text-lg font-semibold text-gray-900 mb-3">What is this?</h2>
          <p className="text-gray-600 leading-relaxed">
            The Iowa Price Transparency tool helps Iowans compare negotiated medical procedure
            prices across hospitals and insurance payers. Under the federal{' '}
            <span className="font-medium">Transparency in Coverage Rule</span>, health insurers
            are required to publish machine-readable files (MRFs) containing their negotiated
            rates with healthcare providers.
          </p>
        </div>

        <div className="card">
          <h2 className="text-lg font-semibold text-gray-900 mb-3">Data Sources</h2>
          <ul className="space-y-2 text-gray-600">
            <li className="flex items-start gap-2">
              <span className="text-primary-600 mt-1">&#8226;</span>
              <span>
                <span className="font-medium">CMS Transparency in Coverage MRFs</span> — negotiated
                rate data published by insurance payers
              </span>
            </li>
            <li className="flex items-start gap-2">
              <span className="text-primary-600 mt-1">&#8226;</span>
              <span>
                <span className="font-medium">NPPES NPI Registry</span> — verified National Provider
                Identifier data for Iowa facilities
              </span>
            </li>
            <li className="flex items-start gap-2">
              <span className="text-primary-600 mt-1">&#8226;</span>
              <span>
                <span className="font-medium">CPT Code Catalog</span> — 88 common medical procedure
                codes across 14 categories
              </span>
            </li>
          </ul>
        </div>

        {stats && (
          <div className="card">
            <h2 className="text-lg font-semibold text-gray-900 mb-3">Current Coverage</h2>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-center">
              <div>
                <div className="text-2xl font-bold text-primary-600">
                  {formatNumber(stats.total_providers)}
                </div>
                <div className="text-xs text-gray-500">Providers</div>
              </div>
              <div>
                <div className="text-2xl font-bold text-primary-600">
                  {formatNumber(stats.total_payers)}
                </div>
                <div className="text-xs text-gray-500">Insurance Payers</div>
              </div>
              <div>
                <div className="text-2xl font-bold text-primary-600">
                  {formatNumber(stats.total_procedures)}
                </div>
                <div className="text-xs text-gray-500">Procedures</div>
              </div>
              <div>
                <div className="text-2xl font-bold text-primary-600">
                  {formatNumber(stats.total_rates)}
                </div>
                <div className="text-xs text-gray-500">Rate Records</div>
              </div>
            </div>
            {stats.last_updated && (
              <p className="text-xs text-gray-400 text-center mt-3">
                Last updated: {new Date(stats.last_updated).toLocaleDateString()}
              </p>
            )}
          </div>
        )}

        <div className="card">
          <h2 className="text-lg font-semibold text-gray-900 mb-3">Important Disclaimers</h2>
          <div className="space-y-2 text-gray-600 text-sm">
            <p>
              This tool shows <span className="font-medium">negotiated rates</span> between
              insurance payers and healthcare providers. These are not the prices you will
              necessarily pay out of pocket.
            </p>
            <p>
              Your actual costs depend on your specific insurance plan, deductible, copay/coinsurance
              amounts, and whether you have met your out-of-pocket maximum.
            </p>
            <p>
              This tool is for informational purposes only and should not be used as the sole
              basis for healthcare decisions. Always consult with your insurance provider and
              healthcare team for accurate cost estimates.
            </p>
          </div>
        </div>

        <div className="card">
          <h2 className="text-lg font-semibold text-gray-900 mb-3">Technology</h2>
          <p className="text-gray-600 text-sm leading-relaxed">
            Built with Python/FastAPI (backend), Next.js/React (frontend), and SQLite.
            Multi-gigabyte MRF files are processed using streaming JSON parsing (ijson)
            to extract Iowa-specific pricing data efficiently.
          </p>
        </div>
      </div>
    </div>
  );
}

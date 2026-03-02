'use client';

import { useEffect, useState } from 'react';
import type { CoverageStats } from '@/lib/types';
import { getCoverageStats } from '@/lib/api';
import { formatNumber } from '@/lib/utils';

interface Section {
  id: string;
  title: string;
}

const SECTIONS: Section[] = [
  { id: 'overview', title: 'Overview' },
  { id: 'data-sources', title: 'Data Sources' },
  { id: 'facility-identification', title: 'Facility Identification' },
  { id: 'rate-extraction', title: 'Rate Extraction' },
  { id: 'medicare-benchmarks', title: 'Medicare Benchmarks' },
  { id: 'data-dictionary', title: 'Data Dictionary' },
  { id: 'limitations', title: 'Limitations & Caveats' },
];

export default function MethodologyPage() {
  const [stats, setStats] = useState<CoverageStats | null>(null);

  useEffect(() => {
    getCoverageStats().then(setStats).catch(() => {});
  }, []);

  return (
    <div className="max-w-5xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      <h1 className="text-3xl font-bold text-gray-900 mb-2">Methodology & Data Dictionary</h1>
      <p className="text-gray-500 mb-8">
        Technical documentation for CFOs, Revenue Cycle VPs, and managed care teams.
      </p>

      {/* Table of contents */}
      <nav className="card mb-8">
        <h2 className="text-sm font-semibold text-gray-500 uppercase tracking-wide mb-3">Contents</h2>
        <div className="flex flex-wrap gap-2">
          {SECTIONS.map((s) => (
            <a
              key={s.id}
              href={`#${s.id}`}
              className="px-3 py-1.5 text-sm text-primary-700 bg-primary-50 rounded-lg hover:bg-primary-100 transition-colors"
            >
              {s.title}
            </a>
          ))}
        </div>
      </nav>

      <div className="space-y-8">
        {/* Overview */}
        <section id="overview" className="card scroll-mt-20">
          <h2 className="text-xl font-bold text-gray-900 mb-4">Overview</h2>
          <p className="text-gray-600 leading-relaxed mb-3">
            Iowa Rate Analyzer aggregates and normalizes negotiated rate data from
            insurance payer Machine-Readable Files (MRFs) published under the federal{' '}
            <span className="font-medium">Transparency in Coverage Rule</span> (45 CFR Parts 147, 148, 156).
            This rule requires group health plans and insurers to publish their negotiated
            rates with healthcare providers in a standardized JSON format.
          </p>
          <p className="text-gray-600 leading-relaxed mb-3">
            The tool focuses exclusively on <span className="font-medium">Iowa hospitals</span> and
            provides three analytical views:
          </p>
          <ul className="space-y-2 text-gray-600 ml-4">
            <li className="flex items-start gap-2">
              <span className="text-primary-600 font-bold mt-0.5">1.</span>
              <span><span className="font-medium">My Hospital</span> — View all negotiated rates for a
                single facility, grouped by procedure and payer, with Medicare benchmark comparisons.</span>
            </li>
            <li className="flex items-start gap-2">
              <span className="text-primary-600 font-bold mt-0.5">2.</span>
              <span><span className="font-medium">Market Position</span> — Rank all Iowa facilities for a
                specific procedure by their median negotiated rate, with percentile positioning.</span>
            </li>
            <li className="flex items-start gap-2">
              <span className="text-primary-600 font-bold mt-0.5">3.</span>
              <span><span className="font-medium">Payer Scorecard</span> — For a single facility, rank
                all payers by their median rate-to-Medicare ratio to identify under- and over-paying
                contracts.</span>
            </li>
          </ul>

          {stats && (
            <div className="mt-6 p-4 bg-gray-50 rounded-lg">
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-center">
                <div>
                  <div className="text-xl font-bold text-primary-600">{formatNumber(stats.total_rates)}</div>
                  <div className="text-xs text-gray-500">Rate Records</div>
                </div>
                <div>
                  <div className="text-xl font-bold text-primary-600">{formatNumber(stats.total_providers)}</div>
                  <div className="text-xs text-gray-500">Providers</div>
                </div>
                <div>
                  <div className="text-xl font-bold text-primary-600">{formatNumber(stats.total_procedures)}</div>
                  <div className="text-xs text-gray-500">Procedures</div>
                </div>
                <div>
                  <div className="text-xl font-bold text-primary-600">{formatNumber(stats.total_payers)}</div>
                  <div className="text-xs text-gray-500">Payers</div>
                </div>
              </div>
            </div>
          )}
        </section>

        {/* Data Sources */}
        <section id="data-sources" className="card scroll-mt-20">
          <h2 className="text-xl font-bold text-gray-900 mb-4">Data Sources</h2>

          <div className="space-y-5">
            <div className="border-l-4 border-primary-400 pl-4">
              <h3 className="font-semibold text-gray-900">Transparency in Coverage MRFs</h3>
              <p className="text-sm text-gray-600 mt-1">
                Negotiated rate data published by each insurance payer per the CMS final rule.
                Files follow the CMS In-Network Rate schema (v1.0/v2.0) and contain per-provider,
                per-procedure negotiated rates. Files are typically multi-gigabyte gzip-compressed
                JSON files that are streamed and parsed using ijson (SAX-style JSON parsing).
              </p>
              <div className="mt-2 text-xs text-gray-500">
                <span className="font-medium">Payers currently indexed:</span> Aetna, Cigna
              </div>
            </div>

            <div className="border-l-4 border-amber-400 pl-4">
              <h3 className="font-semibold text-gray-900">CMS Provider of Services (POS) File</h3>
              <p className="text-sm text-gray-600 mt-1">
                The canonical list of Medicare-certified hospitals, published quarterly by CMS.
                Used to establish the definitive set of Iowa hospitals with their CMS Certification
                Numbers (CCN). We use the Q4 2025 release, filtering to Iowa (state code IA) with
                active termination status (code 00), restricted to CCN ranges 160001-160899
                (short-term acute care) and 161300-161399 (Critical Access Hospitals).
              </p>
              <div className="mt-2 text-xs text-gray-500">
                <span className="font-medium">Source:</span> data.cms.gov, Hospital_and_other.DATA.Q4_2025.csv
              </div>
            </div>

            <div className="border-l-4 border-green-400 pl-4">
              <h3 className="font-semibold text-gray-900">NPPES NPI Registry</h3>
              <p className="text-sm text-gray-600 mt-1">
                The National Plan and Provider Enumeration System maintains the authoritative
                registry of National Provider Identifiers (NPIs). Used to map each hospital&apos;s
                CCN to its NPI(s), verify taxonomy codes, and identify organizational subparts.
                Queried via the NPPES API (v2.1) with organization name, state, and city matching.
              </p>
              <div className="mt-2 text-xs text-gray-500">
                <span className="font-medium">API:</span> npiregistry.cms.hhs.gov/api/
              </div>
            </div>

            <div className="border-l-4 border-purple-400 pl-4">
              <h3 className="font-semibold text-gray-900">Medicare Fee Schedules</h3>
              <p className="text-sm text-gray-600 mt-1">
                CY 2025 Medicare Physician Fee Schedule (MPFS) rates for Iowa Locality 00,
                and CY 2025 Outpatient Prospective Payment System (OPPS) national rates.
                Used as benchmarks for computing rate-to-Medicare ratios (&ldquo;% of Medicare&rdquo;).
              </p>
              <div className="mt-2 text-xs text-gray-500">
                <span className="font-medium">MPFS:</span> Iowa Locality 00 non-facility rates |{' '}
                <span className="font-medium">OPPS:</span> National rates (APC-based)
              </div>
            </div>
          </div>
        </section>

        {/* Facility Identification */}
        <section id="facility-identification" className="card scroll-mt-20">
          <h2 className="text-xl font-bold text-gray-900 mb-4">Facility Identification & NPI Deduplication</h2>

          <p className="text-gray-600 leading-relaxed mb-4">
            A critical challenge in price transparency data is that many hospitals have
            <span className="font-medium"> multiple NPIs</span>. CMS allows hospitals to register
            &ldquo;subpart&rdquo; NPIs for departments, and hospital-owned clinics may have
            their own NPIs with different taxonomy codes. Without deduplication, a single hospital
            could appear as multiple entries with conflicting rates.
          </p>

          <h3 className="font-semibold text-gray-900 mb-2">CCN as Canonical Identifier</h3>
          <p className="text-gray-600 text-sm leading-relaxed mb-4">
            We use the <span className="font-medium">CMS Certification Number (CCN)</span> as the
            unique identifier for each hospital. The CCN is a 6-digit number assigned by CMS to
            each Medicare-certified facility. Unlike NPIs (which can be many-to-one per hospital),
            each hospital has exactly one CCN. Iowa CCNs follow the pattern: <code className="text-xs bg-gray-100 px-1 py-0.5 rounded">16XXXX</code>.
          </p>

          <h3 className="font-semibold text-gray-900 mb-2">NPI-to-CCN Mapping Process</h3>
          <ol className="space-y-3 text-sm text-gray-600 ml-4">
            <li className="flex items-start gap-2">
              <span className="bg-primary-100 text-primary-700 px-2 py-0.5 rounded text-xs font-bold shrink-0">1</span>
              <span>Parse the CMS POS file to get the canonical list of Iowa hospitals (112 active facilities).</span>
            </li>
            <li className="flex items-start gap-2">
              <span className="bg-primary-100 text-primary-700 px-2 py-0.5 rounded text-xs font-bold shrink-0">2</span>
              <span>For each hospital, query the NPPES API by organization name, state (IA), and city to find associated NPIs.</span>
            </li>
            <li className="flex items-start gap-2">
              <span className="bg-primary-100 text-primary-700 px-2 py-0.5 rounded text-xs font-bold shrink-0">3</span>
              <span>Score each NPI match based on name similarity, taxonomy code (282N = General Acute Care Hospital), address, and ZIP code overlap.</span>
            </li>
            <li className="flex items-start gap-2">
              <span className="bg-primary-100 text-primary-700 px-2 py-0.5 rounded text-xs font-bold shrink-0">4</span>
              <span>
                Select one <span className="font-medium">primary NPI</span> per CCN using priority:
                (a) non-subpart NPI with hospital taxonomy, (b) most existing MRF rate records,
                (c) earliest enumeration date.
              </span>
            </li>
            <li className="flex items-start gap-2">
              <span className="bg-primary-100 text-primary-700 px-2 py-0.5 rounded text-xs font-bold shrink-0">5</span>
              <span>All dashboard views use only the primary NPI&apos;s rates, ensuring one row per hospital.</span>
            </li>
          </ol>

          <div className="mt-4 p-3 bg-amber-50 border border-amber-200 rounded-lg text-sm text-amber-800">
            <span className="font-medium">Note:</span> Some small rural hospitals could not be matched
            to NPIs via the NPPES API due to significant name differences between the POS file and
            the NPI registry. These facilities appear in the facility list but may show &ldquo;no rate data.&rdquo;
          </div>
        </section>

        {/* Rate Extraction */}
        <section id="rate-extraction" className="card scroll-mt-20">
          <h2 className="text-xl font-bold text-gray-900 mb-4">Rate Extraction Pipeline</h2>

          <p className="text-gray-600 leading-relaxed mb-4">
            Payer MRF files are multi-gigabyte JSON files that cannot be loaded into memory.
            The extraction pipeline uses a two-phase streaming approach:
          </p>

          <div className="grid md:grid-cols-2 gap-4 mb-4">
            <div className="p-4 bg-blue-50 rounded-lg">
              <h3 className="font-semibold text-blue-900 mb-2">Phase 1: Provider Mapping</h3>
              <p className="text-sm text-blue-800">
                Stream through <code className="text-xs bg-blue-100 px-1 rounded">provider_references</code> to
                identify provider groups containing Iowa NPIs. Build an in-memory map of
                group ID to Iowa NPI list.
              </p>
            </div>
            <div className="p-4 bg-green-50 rounded-lg">
              <h3 className="font-semibold text-green-900 mb-2">Phase 2: Rate Extraction</h3>
              <p className="text-sm text-green-800">
                Stream through <code className="text-xs bg-green-100 px-1 rounded">in_network</code> items,
                filtering to target CPT codes. Cross-join negotiated rates with Iowa providers
                from Phase 1.
              </p>
            </div>
          </div>

          <h3 className="font-semibold text-gray-900 mb-2">Rate Filtering Rules</h3>
          <ul className="space-y-1 text-sm text-gray-600 ml-4">
            <li className="flex items-start gap-2">
              <span className="text-primary-600 mt-0.5">&bull;</span>
              <span>Only <span className="font-medium">fee-for-service (FFS)</span> negotiated rates are included (<code className="text-xs bg-gray-100 px-1 rounded">negotiated_type: &quot;negotiated&quot;</code>)</span>
            </li>
            <li className="flex items-start gap-2">
              <span className="text-primary-600 mt-0.5">&bull;</span>
              <span>Rates of <span className="font-medium">$0.00</span> are excluded (common placeholder in HMO files)</span>
            </li>
            <li className="flex items-start gap-2">
              <span className="text-primary-600 mt-0.5">&bull;</span>
              <span>Duplicate rates (same payer + provider + code + rate + type) are deduplicated at insert time</span>
            </li>
            <li className="flex items-start gap-2">
              <span className="text-primary-600 mt-0.5">&bull;</span>
              <span>Only rates for the 87 tracked CPT codes are extracted (covering common medical, surgical, diagnostic, and emergency procedures)</span>
            </li>
          </ul>
        </section>

        {/* Medicare Benchmarks */}
        <section id="medicare-benchmarks" className="card scroll-mt-20">
          <h2 className="text-xl font-bold text-gray-900 mb-4">Medicare Benchmark Methodology</h2>

          <p className="text-gray-600 leading-relaxed mb-4">
            Rate-to-Medicare ratios (&ldquo;% of Medicare&rdquo;) are the primary metric for
            evaluating payer contract competitiveness. The appropriate Medicare benchmark depends
            on the <span className="font-medium">service setting</span> of the negotiated rate.
          </p>

          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-gray-200">
                  <th className="text-left font-medium text-gray-500 py-2 pr-4">Service Setting</th>
                  <th className="text-left font-medium text-gray-500 py-2 pr-4">Medicare Benchmark</th>
                  <th className="text-left font-medium text-gray-500 py-2">Description</th>
                </tr>
              </thead>
              <tbody className="text-gray-600">
                <tr className="border-b border-gray-100">
                  <td className="py-2 pr-4 font-medium">Institutional / Outpatient / Inpatient</td>
                  <td className="py-2 pr-4">OPPS Rate</td>
                  <td className="py-2">CY 2025 Outpatient Prospective Payment System national rate (APC-based)</td>
                </tr>
                <tr className="border-b border-gray-100">
                  <td className="py-2 pr-4 font-medium">Professional / Ambulatory</td>
                  <td className="py-2 pr-4">MPFS Rate</td>
                  <td className="py-2">CY 2025 Medicare Physician Fee Schedule, Iowa Locality 00, non-facility rate</td>
                </tr>
                <tr>
                  <td className="py-2 pr-4 font-medium">Unknown / Mixed</td>
                  <td className="py-2 pr-4">OPPS (default)</td>
                  <td className="py-2">Falls back to OPPS rate when service setting is not specified</td>
                </tr>
              </tbody>
            </table>
          </div>

          <div className="mt-4 p-3 bg-blue-50 border border-blue-200 rounded-lg text-sm text-blue-800">
            <span className="font-medium">Calculation:</span>{' '}
            <code className="bg-blue-100 px-1.5 py-0.5 rounded">% of Medicare = (Negotiated Rate / Medicare Reference Rate) x 100</code>
            <br />
            A value of 200% means the payer pays twice the Medicare rate. Values below 100%
            indicate payment below Medicare.
          </div>
        </section>

        {/* Data Dictionary */}
        <section id="data-dictionary" className="card scroll-mt-20">
          <h2 className="text-xl font-bold text-gray-900 mb-4">Data Dictionary</h2>

          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b-2 border-gray-200">
                  <th className="text-left font-medium text-gray-500 py-2 pr-4 w-48">Field</th>
                  <th className="text-left font-medium text-gray-500 py-2 pr-4 w-32">Type</th>
                  <th className="text-left font-medium text-gray-500 py-2">Definition</th>
                </tr>
              </thead>
              <tbody className="text-gray-600">
                <DictRow
                  field="CCN"
                  type="Text (6 chars)"
                  def="CMS Certification Number. Unique identifier assigned by CMS to each Medicare-certified hospital. Format: 16XXXX for Iowa."
                />
                <DictRow
                  field="NPI"
                  type="Text (10 digits)"
                  def="National Provider Identifier. A hospital may have multiple NPIs (parent org, subparts, clinics). Only the primary NPI is used for rate lookups."
                />
                <DictRow
                  field="Billing Code (CPT)"
                  type="Text (5 chars)"
                  def="Current Procedural Terminology code identifying a specific medical procedure. Published by the AMA."
                />
                <DictRow
                  field="Negotiated Rate"
                  type="Currency (USD)"
                  def="The dollar amount a payer has agreed to pay a specific provider for a specific procedure. Extracted from payer MRF files."
                />
                <DictRow
                  field="Rate Type"
                  type="Text"
                  def="The type of negotiated rate: 'negotiated' (FFS), 'derived' (calculated), 'fee schedule', or 'percentage'. Only 'negotiated' rates are displayed."
                />
                <DictRow
                  field="Service Setting"
                  type="Text"
                  def="Where the service is performed: 'institutional' / 'outpatient' / 'inpatient' (facility-based) or 'professional' / 'ambulatory' (physician-based). Determines which Medicare benchmark to use."
                />
                <DictRow
                  field="Payer"
                  type="Text"
                  def="The insurance company that published the rate. Identified by name and short_name (e.g., 'aetna', 'cigna')."
                />
                <DictRow
                  field="% of Medicare"
                  type="Integer"
                  def="Negotiated rate expressed as a percentage of the applicable Medicare reference rate. 100% = Medicare rate. >200% typically indicates premium pricing."
                />
                <DictRow
                  field="Median Rate"
                  type="Currency (USD)"
                  def="The middle value when all rates for a facility+procedure combination are sorted. Used in market position rankings because it is less sensitive to outliers than mean."
                />
                <DictRow
                  field="Percentile"
                  type="Integer (0-100)"
                  def="A facility's rank position among all Iowa facilities for a given procedure. 0th percentile = lowest rate, 100th = highest rate."
                />
                <DictRow
                  field="Bed Count"
                  type="Integer"
                  def="Number of certified hospital beds from the CMS POS file. Provides size context for rate comparisons."
                />
                <DictRow
                  field="Ownership Type"
                  type="Text"
                  def="Hospital ownership category from CMS POS file: Nonprofit, For-profit, Government, or Tribal."
                />
                <DictRow
                  field="Hospital Type"
                  type="Text"
                  def="Acute Care (CCN 160001-160899) or Critical Access (CCN 161300-161399). Critical Access Hospitals (CAHs) are small rural hospitals with special Medicare reimbursement."
                />
              </tbody>
            </table>
          </div>
        </section>

        {/* Limitations */}
        <section id="limitations" className="card scroll-mt-20">
          <h2 className="text-xl font-bold text-gray-900 mb-4">Limitations & Caveats</h2>

          <div className="space-y-4">
            <div className="flex items-start gap-3">
              <span className="text-amber-500 text-lg shrink-0">&#9888;</span>
              <div>
                <h3 className="font-semibold text-gray-900 text-sm">Payer Coverage is Incomplete</h3>
                <p className="text-sm text-gray-600">
                  Not all Iowa payers are currently indexed. Large payers like UHC and Wellmark
                  use different NPI types (individual physician NPIs vs. organizational NPIs) or
                  different file formats that are still being integrated. Rate comparisons may be
                  skewed by which payers are included.
                </p>
              </div>
            </div>

            <div className="flex items-start gap-3">
              <span className="text-amber-500 text-lg shrink-0">&#9888;</span>
              <div>
                <h3 className="font-semibold text-gray-900 text-sm">MRF Data Quality Varies</h3>
                <p className="text-sm text-gray-600">
                  Payer-published MRF data may contain errors, stale rates, or rates that do not
                  reflect actual contracted amounts. Some rates may represent fee schedules rather
                  than negotiated contracts. Always validate against your facility&apos;s internal
                  remittance data before using for contract negotiations.
                </p>
              </div>
            </div>

            <div className="flex items-start gap-3">
              <span className="text-amber-500 text-lg shrink-0">&#9888;</span>
              <div>
                <h3 className="font-semibold text-gray-900 text-sm">Medicare Benchmarks Are Approximate</h3>
                <p className="text-sm text-gray-600">
                  OPPS rates shown are national averages (not hospital-specific). Actual Medicare
                  reimbursement varies by hospital wage index, outlier payments, add-on payments,
                  and other adjustments. MPFS rates use Iowa Locality 00 which covers most of
                  the state, but some areas may differ. These benchmarks are best used for relative
                  comparison rather than exact payment prediction.
                </p>
              </div>
            </div>

            <div className="flex items-start gap-3">
              <span className="text-amber-500 text-lg shrink-0">&#9888;</span>
              <div>
                <h3 className="font-semibold text-gray-900 text-sm">Primary NPI May Not Capture All Rates</h3>
                <p className="text-sm text-gray-600">
                  Selecting one primary NPI per hospital simplifies analysis but may exclude rates
                  negotiated under secondary NPIs (subparts or affiliated clinics). If a payer
                  negotiated rates with a hospital&apos;s secondary NPI but not the primary,
                  those rates will not appear in the dashboard.
                </p>
              </div>
            </div>

            <div className="flex items-start gap-3">
              <span className="text-amber-500 text-lg shrink-0">&#9888;</span>
              <div>
                <h3 className="font-semibold text-gray-900 text-sm">Not Financial or Legal Advice</h3>
                <p className="text-sm text-gray-600">
                  This tool is for informational and analytical purposes only. Rate data should be
                  independently verified before use in contract negotiations, financial modeling,
                  or legal proceedings.
                </p>
              </div>
            </div>
          </div>
        </section>
      </div>
    </div>
  );
}

function DictRow({ field, type, def }: { field: string; type: string; def: string }) {
  return (
    <tr className="border-b border-gray-100">
      <td className="py-2.5 pr-4 font-medium text-gray-900">{field}</td>
      <td className="py-2.5 pr-4 text-gray-500 text-xs font-mono">{type}</td>
      <td className="py-2.5 text-gray-600">{def}</td>
    </tr>
  );
}

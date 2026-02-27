import type { CompareResponse } from '@/lib/types';
import StatsSummary from './StatsSummary';
import ProcedureCard from './ProcedureCard';
import EmptyState from './EmptyState';

interface CompareResultsProps {
  data: CompareResponse;
}

export default function CompareResults({ data }: CompareResultsProps) {
  if (data.procedures.length === 0) {
    return (
      <EmptyState
        title="No results found"
        description="Try different codes or adjust your filters."
      />
    );
  }

  return (
    <div className="space-y-6">
      <StatsSummary stats={data.stats} />
      {data.procedures.map((proc) => (
        <ProcedureCard key={proc.billing_code} procedure={proc} />
      ))}
    </div>
  );
}

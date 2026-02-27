import { Suspense } from 'react';
import ProcedureContent from './ProcedureContent';
import LoadingSpinner from '@/components/LoadingSpinner';

export default function ProcedureDetailPage() {
  return (
    <Suspense fallback={<LoadingSpinner className="py-24" />}>
      <ProcedureContent />
    </Suspense>
  );
}

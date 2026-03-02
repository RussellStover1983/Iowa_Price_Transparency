import { Suspense } from 'react';
import DashboardContent from './DashboardContent';
import LoadingSpinner from '@/components/LoadingSpinner';

export default function DashboardPage() {
  return (
    <Suspense fallback={<LoadingSpinner className="py-24" />}>
      <DashboardContent />
    </Suspense>
  );
}

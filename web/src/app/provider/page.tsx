import { Suspense } from 'react';
import ProviderContent from './ProviderContent';
import LoadingSpinner from '@/components/LoadingSpinner';

export default function ProviderDetailPage() {
  return (
    <Suspense fallback={<LoadingSpinner className="py-24" />}>
      <ProviderContent />
    </Suspense>
  );
}

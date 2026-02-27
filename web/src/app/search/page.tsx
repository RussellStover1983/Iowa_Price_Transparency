import { Suspense } from 'react';
import SearchContent from './SearchContent';
import LoadingSpinner from '@/components/LoadingSpinner';

export default function SearchPage() {
  return (
    <Suspense fallback={<LoadingSpinner className="py-24" />}>
      <SearchContent />
    </Suspense>
  );
}

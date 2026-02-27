import CodeChip from './CodeChip';
import type { CptSearchResult } from '@/lib/types';

interface SelectedCodesProps {
  codes: CptSearchResult[];
  onRemove: (code: string) => void;
}

export default function SelectedCodes({ codes, onRemove }: SelectedCodesProps) {
  if (codes.length === 0) return null;

  return (
    <div className="flex flex-wrap gap-2">
      {codes.map((c) => (
        <CodeChip
          key={c.code}
          code={c.code}
          description={c.description}
          onRemove={onRemove}
        />
      ))}
    </div>
  );
}

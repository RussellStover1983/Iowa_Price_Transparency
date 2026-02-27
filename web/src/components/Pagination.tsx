'use client';

interface PaginationProps {
  total: number;
  limit: number;
  offset: number;
  onPageChange: (offset: number) => void;
}

export default function Pagination({ total, limit, offset, onPageChange }: PaginationProps) {
  if (total <= limit) return null;

  const currentPage = Math.floor(offset / limit) + 1;
  const totalPages = Math.ceil(total / limit);

  return (
    <div className="flex items-center justify-center gap-2 mt-6">
      <button
        onClick={() => onPageChange(Math.max(0, offset - limit))}
        disabled={offset === 0}
        className="btn-secondary text-sm px-3 py-1.5 disabled:opacity-40"
      >
        Previous
      </button>
      <span className="text-sm text-gray-600">
        Page {currentPage} of {totalPages}
      </span>
      <button
        onClick={() => onPageChange(offset + limit)}
        disabled={offset + limit >= total}
        className="btn-secondary text-sm px-3 py-1.5 disabled:opacity-40"
      >
        Next
      </button>
    </div>
  );
}

interface CodeChipProps {
  code: string;
  description: string;
  onRemove: (code: string) => void;
}

export default function CodeChip({ code, description, onRemove }: CodeChipProps) {
  return (
    <span className="inline-flex items-center gap-1.5 pl-3 pr-1.5 py-1.5 bg-primary-50 text-primary-700 rounded-full text-sm border border-primary-200">
      <span className="font-mono font-medium text-xs">{code}</span>
      <span className="text-primary-600 truncate max-w-[200px]">{description}</span>
      <button
        onClick={() => onRemove(code)}
        className="ml-0.5 p-0.5 rounded-full hover:bg-primary-200 transition-colors"
        aria-label={`Remove ${code}`}
      >
        <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
        </svg>
      </button>
    </span>
  );
}

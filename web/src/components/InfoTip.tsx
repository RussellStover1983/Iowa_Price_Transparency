'use client';

import { useState, useRef, useEffect } from 'react';

interface InfoTipProps {
  text: string;
  className?: string;
}

export default function InfoTip({ text, className = '' }: InfoTipProps) {
  const [show, setShow] = useState(false);
  const [position, setPosition] = useState<'top' | 'bottom'>('top');
  const triggerRef = useRef<HTMLButtonElement>(null);
  const tipRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (show && triggerRef.current) {
      const rect = triggerRef.current.getBoundingClientRect();
      // If not enough room above, show below
      setPosition(rect.top < 120 ? 'bottom' : 'top');
    }
  }, [show]);

  // Close on click outside
  useEffect(() => {
    if (!show) return;
    const handler = (e: MouseEvent) => {
      if (
        triggerRef.current && !triggerRef.current.contains(e.target as Node) &&
        tipRef.current && !tipRef.current.contains(e.target as Node)
      ) {
        setShow(false);
      }
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, [show]);

  return (
    <span className={`relative inline-flex items-center ${className}`}>
      <button
        ref={triggerRef}
        type="button"
        onClick={() => setShow(!show)}
        onMouseEnter={() => setShow(true)}
        onMouseLeave={() => setShow(false)}
        className="inline-flex items-center justify-center w-4 h-4 text-[10px] font-bold text-gray-400 hover:text-primary-600 border border-gray-300 hover:border-primary-400 rounded-full transition-colors cursor-help"
        aria-label="More information"
      >
        i
      </button>
      {show && (
        <div
          ref={tipRef}
          className={`absolute z-50 w-64 px-3 py-2 text-xs text-gray-700 bg-white border border-gray-200 rounded-lg shadow-lg ${
            position === 'top'
              ? 'bottom-full mb-1.5 left-1/2 -translate-x-1/2'
              : 'top-full mt-1.5 left-1/2 -translate-x-1/2'
          }`}
        >
          {text}
          <a
            href="/methodology#data-dictionary"
            className="block mt-1 text-primary-600 hover:text-primary-700"
            onClick={(e) => e.stopPropagation()}
          >
            View full methodology
          </a>
        </div>
      )}
    </span>
  );
}

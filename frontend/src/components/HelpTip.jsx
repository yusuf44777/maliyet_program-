import { useEffect, useMemo, useRef, useState } from 'react';
import { HelpCircle } from 'lucide-react';

function getPlacementClasses(placement) {
  if (placement === 'right') return 'left-full ml-2 top-1/2 -translate-y-1/2';
  if (placement === 'left') return 'right-full mr-2 top-1/2 -translate-y-1/2';
  if (placement === 'bottom') return 'top-full mt-2 left-1/2 -translate-x-1/2';
  return 'bottom-full mb-2 left-1/2 -translate-x-1/2';
}

export default function HelpTip({
  text,
  title = 'Neden?',
  placement = 'top',
  className = '',
}) {
  const [open, setOpen] = useState(false);
  const [hovered, setHovered] = useState(false);
  const rootRef = useRef(null);

  useEffect(() => {
    if (!open) return;
    const onPointerDown = (event) => {
      if (!rootRef.current?.contains(event.target)) {
        setOpen(false);
      }
    };
    document.addEventListener('mousedown', onPointerDown);
    document.addEventListener('touchstart', onPointerDown);
    return () => {
      document.removeEventListener('mousedown', onPointerDown);
      document.removeEventListener('touchstart', onPointerDown);
    };
  }, [open]);

  const visible = open || hovered;
  const tooltipClasses = useMemo(
    () => getPlacementClasses(placement),
    [placement],
  );

  return (
    <span
      ref={rootRef}
      className={`relative inline-flex items-center ${className}`}
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
    >
      <button
        type="button"
        onClick={() => setOpen((prev) => !prev)}
        className="inline-flex h-5 w-5 items-center justify-center rounded-full border border-blue-200 bg-blue-50 text-blue-600 hover:bg-blue-100 focus:outline-none focus:ring-2 focus:ring-blue-400"
        aria-label={title}
        title={title}
      >
        <HelpCircle className="h-3.5 w-3.5" />
      </button>
      {visible && (
        <div className={`absolute z-30 w-72 max-w-[85vw] rounded-lg border border-blue-200 bg-white p-2.5 text-xs text-gray-700 shadow-lg ${tooltipClasses}`}>
          <p className="font-semibold text-gray-800 mb-1">{title}</p>
          <p>{text}</p>
        </div>
      )}
    </span>
  );
}

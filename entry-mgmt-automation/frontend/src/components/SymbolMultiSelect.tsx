import { useEffect, useMemo, useRef, useState } from 'react'

interface SymbolMultiSelectProps {
  label?: string
  options: string[]
  selected: string[]
  onChange: (nextSelected: string[]) => void
}

export function SymbolMultiSelect({ label = 'Pairs', options, selected, onChange }: SymbolMultiSelectProps) {
  const [open, setOpen] = useState(false)
  const wrapRef = useRef<HTMLDivElement>(null)

  const normalizedOptions = useMemo(() => {
    return Array.from(new Set(options.map((s) => s.trim()).filter(Boolean))).sort()
  }, [options])

  useEffect(() => {
    function onDocMouseDown(ev: MouseEvent) {
      const el = wrapRef.current
      if (!el) return
      if (ev.target instanceof Node && el.contains(ev.target)) return
      setOpen(false)
    }
    document.addEventListener('mousedown', onDocMouseDown)
    return () => document.removeEventListener('mousedown', onDocMouseDown)
  }, [])

  const selectedSet = useMemo(() => new Set(selected), [selected])
  const allSelectedLabel =
    selected.length === 0 ? 'All' : selected.length === 1 ? selected[0] : `${selected.length} selected`

  function toggleSymbol(sym: string) {
    if (selectedSet.has(sym)) {
      onChange(selected.filter((s) => s !== sym))
      return
    }
    onChange([...selected, sym].sort())
  }

  return (
    <div className="symbol-multi-wrap" ref={wrapRef}>
      <button
        type="button"
        className="symbol-multi-trigger"
        disabled={normalizedOptions.length === 0}
        onClick={() => setOpen((v) => !v)}
        title={normalizedOptions.length === 0 ? 'No symbols available' : 'Filter by symbol'}
      >
        {label}: {allSelectedLabel}
      </button>

      {open && (
        <div className="symbol-multi-popover" role="dialog" aria-label="Symbol filter">
          <div className="symbol-multi-actions">
            <button type="button" className="symbol-multi-action-btn" onClick={() => onChange([])}>
              All
            </button>
            <button
              type="button"
              className="symbol-multi-action-btn"
              onClick={() => onChange([])}
              title="Clears selection (same as All)"
            >
              None
            </button>
          </div>
          <div className="symbol-multi-list">
            {normalizedOptions.map((sym) => (
              <label key={sym} className="symbol-multi-item">
                <input
                  type="checkbox"
                  checked={selectedSet.has(sym)}
                  onChange={() => toggleSymbol(sym)}
                />
                <span>{sym}</span>
              </label>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}


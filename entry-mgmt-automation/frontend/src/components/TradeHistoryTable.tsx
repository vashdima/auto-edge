import { useEffect, useRef } from 'react'
import type { EntryMap } from '../types'

function formatDate(iso: string): string {
  try {
    const d = new Date(iso)
    const month = d.toLocaleString('en-US', { month: 'short' })
    const day = d.getDate()
    const year = d.getFullYear()
    const hour = d.getHours()
    const min = d.getMinutes()
    return `${month} ${day}, ${year} ${hour.toString().padStart(2, '0')}:${min.toString().padStart(2, '0')}`
  } catch {
    return iso
  }
}

function formatPrice(n: number): string {
  return n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
}

function formatPips(n: number): string {
  if (!Number.isFinite(n)) return '—'
  const rounded = Math.round(n)
  // Most pip distances should be whole numbers; avoid noisy decimals in the common case.
  if (Math.abs(n - rounded) < 1e-8) return String(rounded)
  return n.toLocaleString('en-US', { maximumFractionDigits: 2 })
}

function formatRr(rr: number | null): string {
  if (rr == null) return '—'
  const sign = rr >= 0 ? '+' : ''
  return `${sign}${rr.toFixed(2)}R`
}

interface TradeHistoryTableProps {
  entries: EntryMap[]
  selectedTradeIds: number[]
  onToggleTradeId: (tradeId: number) => void
  onSelectAll: (checked: boolean) => void
  highlightedIndex: number | null
  onHighlightIndex: (index: number) => void
  /** When false, only Ticker/TF and Entry are shown (for discretion before seeing results). */
  showResultColumns: boolean
}

export function TradeHistoryTable({
  entries,
  selectedTradeIds,
  onToggleTradeId,
  onSelectAll,
  highlightedIndex,
  onHighlightIndex,
  showResultColumns,
}: TradeHistoryTableProps) {
  const headerCheckboxRef = useRef<HTMLInputElement>(null)

  useEffect(() => {
    const el = headerCheckboxRef.current
    if (!el) return
    const someSelected =
      entries.length > 0 &&
      selectedTradeIds.length > 0 &&
      selectedTradeIds.length < entries.length
    el.indeterminate = someSelected
  }, [entries.length, selectedTradeIds.length])

  const allSelected = entries.length > 0 && selectedTradeIds.length === entries.length

  return (
    <div className="trade-table-wrap">
      <h2>Trade History</h2>
      <table className="trade-table">
        <thead>
          <tr>
            <th className="th-select-all">
              <input
                ref={headerCheckboxRef}
                type="checkbox"
                checked={allSelected}
                onChange={() => onSelectAll(!allSelected)}
                aria-label="Select all trades"
              />
            </th>
            <th>Ticker / TF</th>
            <th>Entry</th>
            {showResultColumns && (
              <>
                <th>Exit</th>
                <th>R:R</th>
                <th>Entry Price</th>
                <th>SL</th>
                <th>SL pips</th>
                <th>TP</th>
                <th>Score</th>
              </>
            )}
          </tr>
        </thead>
        <tbody>
          {entries.map((e, i) => (
            <tr
              key={e.trade_id}
              className={[
                selectedTradeIds.includes(e.trade_id) ? 'selected' : '',
                highlightedIndex === i ? 'highlighted' : '',
              ]
                .filter(Boolean)
                .join(' ')}
              onClick={() => onHighlightIndex(i)}
            >
              <td onClick={(ev) => ev.stopPropagation()}>
                <input
                  type="checkbox"
                  checked={selectedTradeIds.includes(e.trade_id)}
                  onChange={() => onToggleTradeId(e.trade_id)}
                  onClick={(ev) => ev.stopPropagation()}
                />
              </td>
              <td>{e.symbol} {e.chartTF}</td>
              <td>{formatDate(e.entryTime)}</td>
              {showResultColumns && (
                <>
                  <td>{e.exitTime ? formatDate(e.exitTime) : '—'}</td>
                  <td>
                    <span
                      className={
                        e.rr != null
                          ? e.rr >= 0
                            ? 'rr-positive'
                            : 'rr-negative'
                          : ''
                      }
                    >
                      {formatRr(e.rr)}
                    </span>
                  </td>
                  <td className="entry-price">{formatPrice(e.entryPrice)}</td>
                  <td className="sl-cell">{formatPrice(e.sl)}</td>
                  <td className="sl-pips-cell">{formatPips(e.slPips)}</td>
                  <td className="tp-cell">{formatPrice(e.tp)}</td>
                  <td>
                    <span
                      className={
                        e.enrichScore != null
                          ? e.enrichScore > 0
                            ? 'score-positive'
                            : e.enrichScore < 0
                              ? 'score-negative'
                              : 'score-zero'
                          : ''
                      }
                    >
                      {e.enrichScore != null ? String(e.enrichScore) : '—'}
                    </span>
                  </td>
                </>
              )}
            </tr>
          ))}
        </tbody>
      </table>
      {entries.length === 0 && (
        <p style={{ color: '#666' }}>No trades. Select a run or run the scanner.</p>
      )}
    </div>
  )
}

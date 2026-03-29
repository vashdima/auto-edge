import type { EntryMap } from '../types'

interface ChartPlaceholderProps {
  label: string
  selectedTrade: EntryMap | null
}

export function ChartPlaceholder({ label, selectedTrade }: ChartPlaceholderProps) {
  return (
    <div className="chart-placeholder">
      <h3>{label}</h3>
      {selectedTrade ? (
        <span>Trade: {selectedTrade.symbol} {selectedTrade.chartTF}</span>
      ) : (
        <span>Select a trade to view chart</span>
      )}
    </div>
  )
}

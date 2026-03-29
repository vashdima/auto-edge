import type { Run } from '../types'

interface RunSelectorProps {
  runs: Run[]
  selectedRunId: number | null
  selectedRunKey: string | null
  loading: boolean
  onSelect: (run: Run | null) => void
}

export function RunSelector({
  runs,
  selectedRunId,
  selectedRunKey,
  loading,
  onSelect,
}: RunSelectorProps) {
  const value =
    selectedRunId != null
      ? String(Number(selectedRunId))
      : selectedRunKey != null
        ? `key:${selectedRunKey}`
        : ''

  return (
    <div className="app-header">
      <label htmlFor="run-select">Run:</label>
      <select
        id="run-select"
        className="run-select"
        value={value}
        disabled={loading}
        onChange={(e) => {
          const v = e.target.value
          if (v === '') {
            onSelect(null)
            return
          }
          if (v.startsWith('key:')) {
            const runKey = v.slice(4)
            const run = runs.find((r) => r.run_key === runKey) ?? null
            onSelect(run)
            return
          }
          const runId = parseInt(v, 10)
          const run = runs.find((r) => r.run_id === runId) ?? null
          onSelect(run)
        }}
      >
        <option value="">Select a run</option>
        {runs.map((r) => (
          <option
            key={r.run_id}
            value={r.run_key != null ? `key:${r.run_key}` : String(Number(r.run_id))}
          >
            {r.run_key ?? `Run ${r.run_id}`}
          </option>
        ))}
      </select>
      {loading && <span>Loading…</span>}
    </div>
  )
}

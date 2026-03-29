import { useCallback, useEffect, useState } from 'react'
import { fetchEntries, fetchRuns } from './api'
import type { EntryMap, Run } from './types'
import { EntryCandlesChart } from './components/EntryCandlesChart'
import { ContextCandlesChart } from './components/ContextCandlesChart'
import { ValidationCandlesChart } from './components/ValidationCandlesChart'
import { RunDashboardPage } from './components/RunDashboardPage'
import { RunSelector } from './components/RunSelector'
import { TradeHistoryTable } from './components/TradeHistoryTable'

const STORAGE_KEY_RUN = 'entry_maps_selected_run'

function loadStoredRun(): { run_key?: string; run_id?: number } | null {
  try {
    const raw = localStorage.getItem(STORAGE_KEY_RUN)
    if (!raw) return null
    return JSON.parse(raw) as { run_key?: string; run_id?: number }
  } catch {
    return null
  }
}

function saveStoredRun(run: Run | null): void {
  if (run == null) {
    localStorage.removeItem(STORAGE_KEY_RUN)
    return
  }
  const payload = run.run_key != null ? { run_key: run.run_key } : { run_id: run.run_id }
  localStorage.setItem(STORAGE_KEY_RUN, JSON.stringify(payload))
}

export function App() {
  const [runs, setRuns] = useState<Run[]>([])
  const [selectedRun, setSelectedRun] = useState<Run | null>(null)
  const [entries, setEntries] = useState<EntryMap[]>([])
  const [selectedTradeIds, setSelectedTradeIds] = useState<number[]>([])
  const [highlightedIndex, setHighlightedIndex] = useState<number | null>(null)
  const [showResults, setShowResults] = useState(false)
  const [showDashboard, setShowDashboard] = useState(false)
  const [loadingRuns, setLoadingRuns] = useState(true)
  const [loadingEntries, setLoadingEntries] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const fetchEntriesForRun = useCallback((run: Run) => {
    setLoadingEntries(true)
    setError(null)
    const runKey = run.run_key ?? undefined
    const runId = run.run_id
    fetchEntries(runKey ? undefined : runId, runKey)
      .then((data) => setEntries(data))
      .catch((err) => setError(String(err.message)))
      .finally(() => setLoadingEntries(false))
  }, [])

  useEffect(() => {
    let cancelled = false
    setLoadingRuns(true)
    setError(null)
    fetchRuns()
      .then((data) => {
        if (cancelled) return
        setRuns(data)
        const stored = loadStoredRun()
        if (stored) {
          const run =
            stored.run_key != null && stored.run_key !== ''
              ? data.find((r) => r.run_key != null && r.run_key === stored.run_key)
              : data.find((r) => Number(r.run_id) === Number(stored.run_id))
          if (run) {
            setSelectedRun(run)
            setLoadingEntries(true)
            fetchEntries(run.run_key ? undefined : run.run_id, run.run_key ?? undefined)
              .then((entriesData) => setEntries(entriesData))
              .catch((err) => setError(String(err.message)))
              .finally(() => setLoadingEntries(false))
          }
        }
      })
      .catch((err) => {
        if (!cancelled) setError(String(err.message))
      })
      .finally(() => {
        if (!cancelled) setLoadingRuns(false)
      })
    return () => {
      cancelled = true
    }
  }, []) // eslint-disable-line react-hooks/exhaustive-deps -- run once on mount

  // Restore selection from storage when runs become available (e.g. after re-fetch or late load)
  useEffect(() => {
    if (runs.length === 0 || selectedRun != null) return
    const stored = loadStoredRun()
    if (!stored) return
    const run =
      stored.run_key != null && stored.run_key !== ''
        ? runs.find((r) => r.run_key != null && r.run_key === stored.run_key)
        : runs.find((r) => Number(r.run_id) === Number(stored.run_id))
    if (run) {
      setSelectedRun(run)
      setLoadingEntries(true)
      fetchEntries(run.run_key ? undefined : run.run_id, run.run_key ?? undefined)
        .then((entriesData) => setEntries(entriesData))
        .catch((err) => setError(String(err.message)))
        .finally(() => setLoadingEntries(false))
    }
  }, [runs, selectedRun])

  const handleRunSelect = useCallback(
    (run: Run | null) => {
      setSelectedRun(run)
      setEntries([])
      setSelectedTradeIds([])
      setHighlightedIndex(null)
      setShowResults(false)
      setShowDashboard(false)
      saveStoredRun(run)
      if (run == null) return
      fetchEntriesForRun(run)
    },
    [fetchEntriesForRun]
  )

  const highlightedTrade =
    highlightedIndex != null ? entries[highlightedIndex] ?? null : null

  const handleToggleTradeId = useCallback((tradeId: number) => {
    setSelectedTradeIds((prev) =>
      prev.includes(tradeId) ? prev.filter((id) => id !== tradeId) : [...prev, tradeId]
    )
  }, [])

  const handleSelectAll = useCallback(
    (checked: boolean) => {
      if (checked) {
        setSelectedTradeIds(entries.map((e) => e.trade_id))
      } else {
        setSelectedTradeIds([])
      }
    },
    [entries]
  )

  const handleExport = useCallback(() => {
    if (selectedTradeIds.length === 0) return
    const runId = selectedRun?.run_id
    if (runId == null) return

    const rows = selectedTradeIds
      .slice()
      .sort((a, b) => a - b)
      .map((tradeId) => `${tradeId},${runId}`)
      .join('\n')

    const csv = `trade_id,run_id\n${rows}\n`
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `picked_trades_run_${runId}.csv`
    document.body.appendChild(a)
    a.click()
    document.body.removeChild(a)
    URL.revokeObjectURL(url)
  }, [selectedRun?.run_id, selectedTradeIds])

  if (showDashboard && selectedRun != null) {
    return <RunDashboardPage run={selectedRun} onBack={() => setShowDashboard(false)} />
  }

  return (
    <div className="app-layout">
      <header className="app-header-section">
        <div className="app-header">
          <RunSelector
            runs={runs}
            selectedRunId={selectedRun?.run_id ?? null}
            selectedRunKey={selectedRun?.run_key ?? null}
            loading={loadingRuns}
            onSelect={handleRunSelect}
          />
          <button
            type="button"
            className="btn-export"
            disabled={selectedTradeIds.length === 0}
            onClick={handleExport}
          >
            Export ({selectedTradeIds.length})
          </button>
          <button
            type="button"
            className="btn-export"
            disabled={selectedRun == null}
            onClick={() => setShowDashboard(true)}
          >
            Open Dashboard
          </button>
        </div>
        {error && (
          <p style={{ color: '#c62828', marginBottom: '0.5rem', marginTop: 0 }}>{error}</p>
        )}
        {loadingEntries && (
          <p style={{ color: '#666', marginBottom: '0.5rem', marginTop: 0 }}>Loading entries…</p>
        )}
        {selectedRun != null && entries.length > 0 && !showResults && (
          <button
            type="button"
            className="btn-show-results"
            onClick={() => setShowResults(true)}
          >
            Show results
          </button>
        )}
        {selectedRun != null && showResults && (() => {
          const runLabel = selectedRun.run_key ?? `Run ${selectedRun.run_id}`
          const pct = (n: number, total: number) => (total > 0 ? Math.round((n / total) * 100) : 0)
          const summary = (list: EntryMap[]) => {
            const totalTrades = list.length
            const totalRR = list.reduce((sum, e) => sum + (e.rr ?? 0), 0)
            const w = list.filter((e) => e.exitReason === 'TP').length
            const l = list.filter((e) => e.exitReason === 'SL').length
            const be = list.filter((e) => e.exitReason === 'BE').length
            return { totalTrades, totalRR, w, l, be, pctW: pct(w, totalTrades), pctL: pct(l, totalTrades), pctBe: pct(be, totalTrades) }
          }
          const all = summary(entries)
          const ticked = summary(entries.filter((e) => selectedTradeIds.includes(e.trade_id)))
          return (
            <div className="run-summary-block">
              <button
                type="button"
                className="btn-hide-results"
                onClick={() => setShowResults(false)}
              >
                Hide results
              </button>
              <p className="run-summary">
                All: run_id: {runLabel}, Total Trades: {all.totalTrades}, Total RR: {all.totalRR.toFixed(1)}, W: {all.pctW}%({all.w}), L: {all.pctL}%({all.l}), BE: {all.pctBe}%({all.be})
              </p>
              <p className="run-summary run-summary-ticked">
                Ticked ({selectedTradeIds.length}): Total Trades: {ticked.totalTrades}, Total RR: {ticked.totalRR.toFixed(1)}, W: {ticked.pctW}%({ticked.w}), L: {ticked.pctL}%({ticked.l}), BE: {ticked.pctBe}%({ticked.be})
              </p>
            </div>
          )
        })()}
      </header>

      <div className="chart-row">
        <ContextCandlesChart trade={highlightedTrade} />
        <ValidationCandlesChart trade={highlightedTrade} />
        <EntryCandlesChart trade={highlightedTrade} />
      </div>

      <section className="trade-section">
        <TradeHistoryTable
          entries={entries}
          selectedTradeIds={selectedTradeIds}
          onToggleTradeId={handleToggleTradeId}
          onSelectAll={handleSelectAll}
          highlightedIndex={highlightedIndex}
          onHighlightIndex={setHighlightedIndex}
          showResultColumns={showResults}
        />
      </section>
    </div>
  )
}

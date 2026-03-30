import { useEffect, useMemo, useRef, useState } from 'react'
import { LineSeries, createChart, type IChartApi, type UTCTimestamp } from 'lightweight-charts'
import { downloadRunConfig, fetchRunStats } from '../api'
import type { HourlyEntryStat, Run, RunStats } from '../types'

type MonteCarloMode = 'bootstrap' | 'shuffle'
type HourlySortKey = 'hour' | 'trades' | 'wins' | 'losses' | 'breakevens' | 'winRate' | 'avgRR' | 'totalRR'
type SortDirection = 'asc' | 'desc'

interface RunDashboardPageProps {
  run: Run
  onBack: () => void
}

function fmt(n: number, digits = 2): string {
  return Number.isFinite(n) ? n.toFixed(digits) : '0.00'
}

function fmtOrDash(value: number | null | undefined, digits: number): string {
  if (value == null || !Number.isFinite(value)) return '—'
  return value.toFixed(digits)
}

function toUnixSeconds(iso: string, fallback: number): number {
  const ms = Date.parse(iso)
  if (!Number.isFinite(ms)) return fallback
  return Math.floor(ms / 1000)
}

function buildTimeSeriesFromEquity(
  stats: RunStats | null,
  pickValue: (point: RunStats['equityCurve'][number]) => number,
): { time: UTCTimestamp; value: number }[] {
  if (!stats?.equityCurve?.length) return []
  let lastTs = 0
  const safeEpochBase = 946684800 // 2000-01-01 UTC, avoids 1970 fallback labels
  return stats.equityCurve.map((p, i) => {
    const fallback = lastTs > 0 ? lastTs + 60 : safeEpochBase + i * 60
    let ts = toUnixSeconds(p.time, fallback)
    if (ts <= lastTs) ts = lastTs + 60
    lastTs = ts
    return {
      time: ts as UTCTimestamp,
      value: pickValue(p),
    }
  })
}

function seededRng(seed: number): () => number {
  let state = seed >>> 0
  return () => {
    state = (1664525 * state + 1013904223) >>> 0
    return state / 0x100000000
  }
}

function percentile(sortedValues: number[], p: number): number {
  if (sortedValues.length === 0) return 0
  const idx = (sortedValues.length - 1) * p
  const lo = Math.floor(idx)
  const hi = Math.ceil(idx)
  if (lo === hi) return sortedValues[lo]
  const frac = idx - lo
  return sortedValues[lo] + (sortedValues[hi] - sortedValues[lo]) * frac
}

function toTwoDigitHour(hour: number): string {
  return String(hour).padStart(2, '0')
}

function heatmapCellColor(avgRR: number, hasTrades: boolean): string {
  if (!hasTrades) return '#f5f5f5'
  const magnitude = Math.min(Math.abs(avgRR) / 2, 1)
  const alpha = 0.15 + magnitude * 0.55
  if (avgRR > 0) return `rgba(46, 125, 50, ${alpha.toFixed(3)})`
  if (avgRR < 0) return `rgba(198, 40, 40, ${alpha.toFixed(3)})`
  return '#eceff1'
}

function buildMonteCarloPercentiles(
  rrSeries: number[],
  simCount: number,
  mode: MonteCarloMode,
  seed: number,
): { p10: number[]; p50: number[]; p90: number[] } {
  const n = rrSeries.length
  if (n === 0 || simCount <= 0) return { p10: [], p50: [], p90: [] }

  const perStepValues: number[][] = Array.from({ length: n }, () => [])
  const rng = seededRng(seed)

  for (let s = 0; s < simCount; s += 1) {
    let path: number[]
    if (mode === 'bootstrap') {
      path = Array.from({ length: n }, () => rrSeries[Math.floor(rng() * n)])
    } else {
      path = rrSeries.slice()
      for (let i = n - 1; i > 0; i -= 1) {
        const j = Math.floor(rng() * (i + 1))
        const tmp = path[i]
        path[i] = path[j]
        path[j] = tmp
      }
    }

    let equity = 0
    for (let i = 0; i < n; i += 1) {
      equity += path[i]
      perStepValues[i].push(equity)
    }
  }

  const p10: number[] = []
  const p50: number[] = []
  const p90: number[] = []
  for (let i = 0; i < n; i += 1) {
    const sorted = perStepValues[i].sort((a, b) => a - b)
    p10.push(percentile(sorted, 0.1))
    p50.push(percentile(sorted, 0.5))
    p90.push(percentile(sorted, 0.9))
  }
  return { p10, p50, p90 }
}

export function RunDashboardPage({ run, onBack }: RunDashboardPageProps) {
  const [stats, setStats] = useState<RunStats | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [mcMode, setMcMode] = useState<MonteCarloMode>('bootstrap')
  const [hourlySortKey, setHourlySortKey] = useState<HourlySortKey>('hour')
  const [hourlySortDirection, setHourlySortDirection] = useState<SortDirection>('asc')
  const [configDownloadError, setConfigDownloadError] = useState<string | null>(null)
  const simCount = useMemo(() => {
    const n = stats?.rrSeries?.length ?? 0
    return n > 150 ? 200 : 500
  }, [stats?.rrSeries?.length])

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    setStats(null)
    fetchRunStats(run.run_key ? undefined : run.run_id, run.run_key ?? undefined)
      .then((data) => {
        if (cancelled) return
        setStats(data)
      })
      .catch((err) => {
        if (cancelled) return
        setError(String(err.message))
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => {
      cancelled = true
    }
  }, [run.run_id, run.run_key])

  const runLabel = run.run_key ?? `Run ${run.run_id}`

  const equityContainerRef = useRef<HTMLDivElement>(null)
  const drawdownContainerRef = useRef<HTMLDivElement>(null)
  const monteCarloContainerRef = useRef<HTMLDivElement>(null)
  const equityChartRef = useRef<IChartApi | null>(null)
  const drawdownChartRef = useRef<IChartApi | null>(null)
  const monteCarloChartRef = useRef<IChartApi | null>(null)

  const equityData = useMemo(() => {
    return buildTimeSeriesFromEquity(stats, (p) => p.equity)
  }, [stats])

  const drawdownData = useMemo(() => {
    return buildTimeSeriesFromEquity(stats, (p) => -p.drawdown)
  }, [stats])

  const realizedEquityData = useMemo(() => buildTimeSeriesFromEquity(stats, (p) => p.equity), [stats])

  const monteCarlo = useMemo(() => {
    if (!stats?.rrSeries?.length) return { p10: [], p50: [], p90: [] }
    return buildMonteCarloPercentiles(
      stats.rrSeries,
      simCount,
      mcMode,
      (run.run_id * 97 + 1337) >>> 0,
    )
  }, [stats, simCount, mcMode, run.run_id])

  const monteCarloP10Data = useMemo(() => {
    if (!realizedEquityData.length) return []
    return monteCarlo.p10.map((v, i) => ({ time: realizedEquityData[i]?.time ?? realizedEquityData[realizedEquityData.length - 1].time, value: v }))
  }, [monteCarlo, realizedEquityData])
  const monteCarloP50Data = useMemo(() => {
    if (!realizedEquityData.length) return []
    return monteCarlo.p50.map((v, i) => ({ time: realizedEquityData[i]?.time ?? realizedEquityData[realizedEquityData.length - 1].time, value: v }))
  }, [monteCarlo, realizedEquityData])
  const monteCarloP90Data = useMemo(() => {
    if (!realizedEquityData.length) return []
    return monteCarlo.p90.map((v, i) => ({ time: realizedEquityData[i]?.time ?? realizedEquityData[realizedEquityData.length - 1].time, value: v }))
  }, [monteCarlo, realizedEquityData])

  const hourlyStats = useMemo<HourlyEntryStat[]>(() => {
    const rows = stats?.hourlyByEntryUtc ?? []
    if (rows.length === 24) return rows
    const byHour = new Map<number, HourlyEntryStat>()
    for (const row of rows) byHour.set(row.hour, row)
    return Array.from({ length: 24 }, (_, hour) => {
      return (
        byHour.get(hour) ?? {
          hour,
          trades: 0,
          wins: 0,
          losses: 0,
          breakevens: 0,
          winRate: 0,
          avgRR: 0,
          totalRR: 0,
        }
      )
    })
  }, [stats])

  const sortedHourlyStats = useMemo<HourlyEntryStat[]>(() => {
    const rows = hourlyStats.slice()
    const dir = hourlySortDirection === 'asc' ? 1 : -1
    rows.sort((a, b) => {
      const aValue = a[hourlySortKey]
      const bValue = b[hourlySortKey]
      if (aValue < bValue) return -1 * dir
      if (aValue > bValue) return 1 * dir
      return a.hour - b.hour
    })
    return rows
  }, [hourlyStats, hourlySortDirection, hourlySortKey])

  function onHourlyHeaderClick(key: HourlySortKey): void {
    if (hourlySortKey === key) {
      setHourlySortDirection((prev) => (prev === 'asc' ? 'desc' : 'asc'))
      return
    }
    setHourlySortKey(key)
    setHourlySortDirection(key === 'hour' ? 'asc' : 'desc')
  }

  function getHourlySortIndicator(key: HourlySortKey): string {
    if (hourlySortKey !== key) return ''
    return hourlySortDirection === 'asc' ? ' ^' : ' v'
  }

  useEffect(() => {
    const el = equityContainerRef.current
    if (!el || equityData.length === 0) return

    const existing = equityChartRef.current
    if (existing) {
      existing.remove()
      equityChartRef.current = null
    }

    const chart = createChart(el, {
      autoSize: true,
      layout: { background: { color: 'white' }, textColor: '#111' },
      rightPriceScale: { borderVisible: false },
      timeScale: { borderVisible: false, rightOffset: 4, timeVisible: true, secondsVisible: false },
      grid: { vertLines: { color: '#f0f0f0' }, horzLines: { color: '#f0f0f0' } },
    })
    const series = chart.addSeries(LineSeries, {
      color: '#1565c0',
      lineWidth: 2,
      priceLineVisible: false,
      lastValueVisible: true,
    })
    series.setData(equityData)
    chart.timeScale().fitContent()
    equityChartRef.current = chart

    const ro = new ResizeObserver(() => chart.timeScale().fitContent())
    ro.observe(el)
    return () => {
      ro.disconnect()
      chart.remove()
      equityChartRef.current = null
    }
  }, [equityData])

  useEffect(() => {
    const el = drawdownContainerRef.current
    if (!el || drawdownData.length === 0) return

    const existing = drawdownChartRef.current
    if (existing) {
      existing.remove()
      drawdownChartRef.current = null
    }

    const chart = createChart(el, {
      autoSize: true,
      layout: { background: { color: 'white' }, textColor: '#111' },
      rightPriceScale: { borderVisible: false },
      timeScale: { borderVisible: false, rightOffset: 4, timeVisible: true, secondsVisible: false },
      grid: { vertLines: { color: '#f0f0f0' }, horzLines: { color: '#f0f0f0' } },
    })
    const series = chart.addSeries(LineSeries, {
      color: '#c62828',
      lineWidth: 2,
      priceLineVisible: false,
      lastValueVisible: true,
    })
    series.setData(drawdownData)
    chart.timeScale().fitContent()
    drawdownChartRef.current = chart

    const ro = new ResizeObserver(() => chart.timeScale().fitContent())
    ro.observe(el)
    return () => {
      ro.disconnect()
      chart.remove()
      drawdownChartRef.current = null
    }
  }, [drawdownData])

  useEffect(() => {
    const el = monteCarloContainerRef.current
    if (!el || monteCarloP50Data.length === 0) return

    const existing = monteCarloChartRef.current
    if (existing) {
      existing.remove()
      monteCarloChartRef.current = null
    }

    const chart = createChart(el, {
      autoSize: true,
      layout: { background: { color: 'white' }, textColor: '#111' },
      rightPriceScale: { borderVisible: false },
      timeScale: { borderVisible: false, rightOffset: 4, timeVisible: true, secondsVisible: false },
      grid: { vertLines: { color: '#f0f0f0' }, horzLines: { color: '#f0f0f0' } },
    })

    const p10 = chart.addSeries(LineSeries, {
      color: '#9e9e9e',
      lineWidth: 1,
      priceLineVisible: false,
      lastValueVisible: false,
    })
    const p90 = chart.addSeries(LineSeries, {
      color: '#9e9e9e',
      lineWidth: 1,
      priceLineVisible: false,
      lastValueVisible: false,
    })
    const p50 = chart.addSeries(LineSeries, {
      color: '#7b1fa2',
      lineWidth: 2,
      priceLineVisible: false,
      lastValueVisible: true,
    })
    const realized = chart.addSeries(LineSeries, {
      color: '#1565c0',
      lineWidth: 2,
      lineStyle: 2,
      priceLineVisible: false,
      lastValueVisible: true,
    })

    p10.setData(monteCarloP10Data)
    p90.setData(monteCarloP90Data)
    p50.setData(monteCarloP50Data)
    realized.setData(realizedEquityData)
    chart.timeScale().fitContent()
    monteCarloChartRef.current = chart

    const ro = new ResizeObserver(() => chart.timeScale().fitContent())
    ro.observe(el)
    return () => {
      ro.disconnect()
      chart.remove()
      monteCarloChartRef.current = null
    }
  }, [monteCarloP10Data, monteCarloP50Data, monteCarloP90Data, realizedEquityData])

  return (
    <div className="app-layout dashboard-scroll-layout">
      <header className="app-header-section">
        <div className="app-header">
          <button type="button" className="btn-hide-results" onClick={onBack}>
            Back to run
          </button>
          <h2 style={{ margin: 0 }}>Run Dashboard: {runLabel}</h2>
          <button
            type="button"
            className="btn-export"
            disabled={run.has_config !== undefined && run.has_config === false}
            title={
              run.has_config === false
                ? 'This run has no stored config (created before snapshots were saved)'
                : 'Download config.yaml used for this run'
            }
            onClick={() => {
              setConfigDownloadError(null)
              downloadRunConfig(run).catch((err) => setConfigDownloadError(String((err as Error).message)))
            }}
          >
            Download config
          </button>
        </div>
      </header>

      {loading && <p style={{ color: '#666' }}>Loading run stats…</p>}
      {error && <p style={{ color: '#c62828' }}>{error}</p>}
      {configDownloadError && <p style={{ color: '#c62828' }}>{configDownloadError}</p>}

      {!loading && !error && stats && (
        <>
          <section className="dashboard-kpi-grid">
          <div className="dashboard-card">
            <h3>Risk-adjusted (per trade)</h3>
            <p>Sharpe (per trade): {fmtOrDash(stats.riskAdjusted.sharpePerTrade, 3)}</p>
            <p>Sortino (per trade): {fmtOrDash(stats.riskAdjusted.sortinoPerTrade, 3)}</p>
            <p>Profit factor: {fmtOrDash(stats.riskAdjusted.profitFactor, 3)}</p>
            <p>σ (R): {fmtOrDash(stats.riskAdjusted.stdDevR, 3)}</p>
            <p className="dashboard-note">Per-trade μ/σ on R-multiples; not annualized.</p>
          </div>

          <div className="dashboard-card">
            <h3>Summary</h3>
            <p>Total trades: {stats.summary.totalTrades}</p>
            <p>Wins/Losses/BE: {stats.summary.wins}/{stats.summary.losses}/{stats.summary.breakevens}</p>
            <p>Win rate: {fmt(stats.summary.winRate, 1)}%</p>
          </div>

          <div className="dashboard-card">
            <h3>R Metrics</h3>
            <p>Total RR: {fmt(stats.summary.totalRR, 2)}</p>
            <p>Avg RR: {fmt(stats.summary.avgRR, 3)}</p>
            <p>Expectancy: {fmt(stats.summary.expectancy, 3)}</p>
          </div>

          <div className="dashboard-card">
            <h3>Risk</h3>
            <p>Max drawdown (R): {fmt(-stats.drawdown.maxDrawdownR, 2)}</p>
            <p>Max losing streak: {stats.streaks.maxLosingStreak}</p>
            <p>Max winning streak: {stats.streaks.maxWinningStreak}</p>
          </div>
          </section>

          <section className="dashboard-2col-grid">
          <div className="dashboard-column">
            <div className="dashboard-card">
              <h3>Equity Curve</h3>
              {equityData.length === 0 ? <p style={{ color: '#666' }}>No data.</p> : <div ref={equityContainerRef} className="dashboard-chart-canvas" />}
            </div>

            <div className="dashboard-card">
              <h3>Drawdown Curve (R)</h3>
              {drawdownData.length === 0 ? <p style={{ color: '#666' }}>No data.</p> : <div ref={drawdownContainerRef} className="dashboard-chart-canvas" />}
            </div>
          </div>

          <div className="dashboard-column">
            <div className="dashboard-card">
              <div className="dashboard-card-header-row">
                <h3>Monte Carlo (Percentile Bands + Realized)</h3>
                <div className="mc-toggle">
                  <button
                    type="button"
                    className={`mc-toggle-btn ${mcMode === 'bootstrap' ? 'active' : ''}`}
                    onClick={() => setMcMode('bootstrap')}
                  >
                    Bootstrap
                  </button>
                  <button
                    type="button"
                    className={`mc-toggle-btn ${mcMode === 'shuffle' ? 'active' : ''}`}
                    onClick={() => setMcMode('shuffle')}
                  >
                    Shuffle only
                  </button>
                </div>
              </div>
              <p className="dashboard-note">
                Simulations: {simCount} | Bands: P10/P50/P90 | Mode: {mcMode}
              </p>
              {monteCarloP50Data.length === 0 ? (
                <p style={{ color: '#666' }}>No data.</p>
              ) : (
                <div ref={monteCarloContainerRef} className="dashboard-chart-canvas" />
              )}
            </div>

            <div className="dashboard-card">
              <h3>Hourly Outcomes (Entry UTC)</h3>
              <div className="hourly-heatmap-grid">
                {hourlyStats.map((row) => (
                  <div
                    key={row.hour}
                    className={`hourly-heatmap-cell ${row.trades < 5 ? 'low-sample' : ''}`}
                    style={{ background: heatmapCellColor(row.avgRR, row.trades > 0) }}
                    title={`${toTwoDigitHour(row.hour)}:00 UTC | n=${row.trades} | W/L/BE ${row.wins}/${row.losses}/${row.breakevens} | Win ${fmt(row.winRate, 1)}% | AvgRR ${fmt(row.avgRR, 3)} | TotalRR ${fmt(row.totalRR, 2)}`}
                  >
                    <span className="hourly-heatmap-hour">{toTwoDigitHour(row.hour)}</span>
                    <span className="hourly-heatmap-value">{fmt(row.avgRR, 2)}</span>
                    <span className="hourly-heatmap-count">n={row.trades}</span>
                  </div>
                ))}
              </div>
              <div className="hourly-table-wrap">
                <table className="hourly-table">
                  <thead>
                    <tr>
                      <th>
                        <button type="button" onClick={() => onHourlyHeaderClick('hour')}>
                          Hour{getHourlySortIndicator('hour')}
                        </button>
                      </th>
                      <th>
                        <button type="button" onClick={() => onHourlyHeaderClick('trades')}>
                          Trades{getHourlySortIndicator('trades')}
                        </button>
                      </th>
                      <th>
                        <button type="button" onClick={() => onHourlyHeaderClick('wins')}>
                          W{getHourlySortIndicator('wins')}
                        </button>
                      </th>
                      <th>
                        <button type="button" onClick={() => onHourlyHeaderClick('losses')}>
                          L{getHourlySortIndicator('losses')}
                        </button>
                      </th>
                      <th>
                        <button type="button" onClick={() => onHourlyHeaderClick('breakevens')}>
                          BE{getHourlySortIndicator('breakevens')}
                        </button>
                      </th>
                      <th>
                        <button type="button" onClick={() => onHourlyHeaderClick('winRate')}>
                          Win%{getHourlySortIndicator('winRate')}
                        </button>
                      </th>
                      <th>
                        <button type="button" onClick={() => onHourlyHeaderClick('avgRR')}>
                          Avg RR{getHourlySortIndicator('avgRR')}
                        </button>
                      </th>
                      <th>
                        <button type="button" onClick={() => onHourlyHeaderClick('totalRR')}>
                          Total RR{getHourlySortIndicator('totalRR')}
                        </button>
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {sortedHourlyStats.map((row) => (
                      <tr key={row.hour} className={row.trades < 5 ? 'low-sample' : ''}>
                        <td>{toTwoDigitHour(row.hour)}:00</td>
                        <td>{row.trades}</td>
                        <td>{row.wins}</td>
                        <td>{row.losses}</td>
                        <td>{row.breakevens}</td>
                        <td>{fmt(row.winRate, 1)}%</td>
                        <td>{fmt(row.avgRR, 3)}</td>
                        <td>{fmt(row.totalRR, 2)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
          </section>
        </>
      )}
    </div>
  )
}


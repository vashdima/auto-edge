import { useEffect, useMemo, useRef } from 'react'
import { createChart, CandlestickSeries, LineSeries, type IChartApi, type UTCTimestamp } from 'lightweight-charts'
import type { EntryMap } from '../types'

type Mode = 'upToEntry'

function toUnixSeconds(isoTime: string): number {
  const ms = Date.parse(isoTime)
  return Math.floor(ms / 1000)
}

function sliceContextUpToEntry(trade: EntryMap) {
  const entryMs = Date.parse(trade.entryTime)
  return trade.contextBuffer.filter((b) => Date.parse(b.time) <= entryMs)
}

export function ContextCandlesChart({ trade }: { trade: EntryMap | null }) {
  const containerRef = useRef<HTMLDivElement>(null)
  const chartRef = useRef<IChartApi | null>(null)
  const seriesRef = useRef<ReturnType<IChartApi['addSeries']> | null>(null)
  const emaSlowRef = useRef<ReturnType<IChartApi['addSeries']> | null>(null)
  const emaMediumRef = useRef<ReturnType<IChartApi['addSeries']> | null>(null)
  const emaFastRef = useRef<ReturnType<IChartApi['addSeries']> | null>(null)
  const mode: Mode = 'upToEntry'

  const { candles, emaSlow, emaMedium, emaFast } = useMemo(() => {
    if (!trade) {
      return { candles: [], emaSlow: [], emaMedium: [], emaFast: [] }
    }
    const entryMs = Date.parse(trade.entryTime)
    const buf = mode === 'upToEntry' ? sliceContextUpToEntry(trade) : trade.contextBuffer

    let lastIndexBeforeEntry = -1
    buf.forEach((b, idx) => {
      const t = Date.parse(b.time)
      if (!Number.isNaN(t) && t < entryMs) {
        lastIndexBeforeEntry = idx
      }
    })

    const candlesData = buf
      .filter((b) => b.open != null && b.high != null && b.low != null && b.close != null)
      .map((b, idx) => {
        const time = toUnixSeconds(b.time) as UTCTimestamp
        const open = b.open as number
        let high = b.high as number
        let low = b.low as number
        let close = b.close as number

        if (idx === lastIndexBeforeEntry) {
          const entryPrice = trade.entryPrice
          close = entryPrice
          if (entryPrice > high) high = entryPrice
          if (entryPrice < low) low = entryPrice
        }

        return { time, open, high, low, close }
      })

    const emaSlowData = buf
      .filter((b) => b.ema_slow != null)
      .map((b) => ({
        time: toUnixSeconds(b.time) as UTCTimestamp,
        value: b.ema_slow as number,
      }))

    const emaMediumData = buf
      .filter((b) => b.ema_medium != null)
      .map((b) => ({
        time: toUnixSeconds(b.time) as UTCTimestamp,
        value: b.ema_medium as number,
      }))

    const emaFastData = buf
      .filter((b) => b.ema_fast != null)
      .map((b) => ({
        time: toUnixSeconds(b.time) as UTCTimestamp,
        value: b.ema_fast as number,
      }))

    return {
      candles: candlesData,
      emaSlow: emaSlowData,
      emaMedium: emaMediumData,
      emaFast: emaFastData,
    }
  }, [trade, mode])

  useEffect(() => {
    if (trade == null) {
      const chart = chartRef.current
      if (chart) {
        chart.remove()
        chartRef.current = null
        seriesRef.current = null
        emaSlowRef.current = null
        emaMediumRef.current = null
        emaFastRef.current = null
      }
      return
    }

    const el = containerRef.current
    if (!el) return

    const chart = chartRef.current
    if (chart) {
      const series = seriesRef.current
      if (series) {
        series.setData(candles)
      }
      if (emaSlowRef.current) {
        emaSlowRef.current.setData(emaSlow)
      }
      if (emaMediumRef.current) {
        emaMediumRef.current.setData(emaMedium)
      }
      if (emaFastRef.current) {
        emaFastRef.current.setData(emaFast)
      }
      chart.timeScale().fitContent()
      return
    }

    const newChart = createChart(el, {
      autoSize: true,
      layout: { background: { color: 'white' }, textColor: '#111' },
      rightPriceScale: { borderVisible: false },
      timeScale: { borderVisible: false, rightOffset: 12 },
      grid: { vertLines: { color: '#f0f0f0' }, horzLines: { color: '#f0f0f0' } },
    })
    chartRef.current = newChart
    const newSeries = newChart.addSeries(CandlestickSeries, {
      upColor: '#ffffff',
      borderUpColor: '#000000',
      wickUpColor: '#000000',
      downColor: '#333333',
      borderDownColor: '#333333',
      wickDownColor: '#333333',
    })
    seriesRef.current = newSeries

    const slowSeries = newChart.addSeries(LineSeries, {
      color: '#000000',
      lineWidth: 1,
      priceLineVisible: false,
      lastValueVisible: false,
    })
    const mediumSeries = newChart.addSeries(LineSeries, {
      color: '#1565c0',
      lineWidth: 1,
      priceLineVisible: false,
      lastValueVisible: false,
    })
    const fastSeries = newChart.addSeries(LineSeries, {
      color: '#c62828',
      lineWidth: 1,
      priceLineVisible: false,
      lastValueVisible: false,
    })
    emaSlowRef.current = slowSeries
    emaMediumRef.current = mediumSeries
    emaFastRef.current = fastSeries

    newSeries.setData(candles)
    slowSeries.setData(emaSlow)
    mediumSeries.setData(emaMedium)
    fastSeries.setData(emaFast)
    newChart.timeScale().fitContent()

    const ro = new ResizeObserver(() => {
      newChart.timeScale().fitContent()
    })
    ro.observe(el)

    return () => {
      ro.disconnect()
      newChart.remove()
      chartRef.current = null
      seriesRef.current = null
      emaSlowRef.current = null
      emaMediumRef.current = null
      emaFastRef.current = null
    }
  }, [trade, candles, emaSlow, emaMedium, emaFast])

  if (!trade) {
    return (
      <div className="chart-placeholder">
        <h3>Context TF</h3>
        <span>Select a trade to view context</span>
      </div>
    )
  }

  const hasEmaData = emaSlow.length > 0 || emaMedium.length > 0 || emaFast.length > 0

  return (
    <div className="chart-panel">
      <div className="chart-panel-header">
        <h3>Context TF (up to entry)</h3>
        <span className="chart-panel-subtitle">{trade.symbol} {trade.contextTF}</span>
        {!hasEmaData && (
          <span className="chart-panel-subtitle chart-panel-hint" title="Run scanner_indicators.py to populate EMAs in the DB">
            No context EMA data — run pipeline: mtf_loader → scanner_indicators → scanner_entry_mgmt
          </span>
        )}
      </div>
      <div ref={containerRef} className="chart-canvas" />
    </div>
  )
}


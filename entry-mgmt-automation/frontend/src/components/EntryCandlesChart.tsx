import { useEffect, useMemo, useRef } from 'react'
import { createChart, CandlestickSeries, LineSeries, type IChartApi, type UTCTimestamp } from 'lightweight-charts'
import type { EntryMap } from '../types'

type Mode = 'upToEntry'

function toUnixSeconds(isoTime: string): number {
  const ms = Date.parse(isoTime)
  return Math.floor(ms / 1000)
}

function sliceUpToEntry(trade: EntryMap): EntryMap['chartBuffer'] {
  const entryMs = Date.parse(trade.entryTime)
  return trade.chartBuffer.filter((b) => Date.parse(b.time) <= entryMs)
}

export function EntryCandlesChart({ trade }: { trade: EntryMap | null }) {
  const containerRef = useRef<HTMLDivElement>(null)
  const chartRef = useRef<IChartApi | null>(null)
  const seriesRef = useRef<ReturnType<IChartApi['addSeries']> | null>(null)
  const emaSlowRef = useRef<ReturnType<IChartApi['addSeries']> | null>(null)
  const emaMediumRef = useRef<ReturnType<IChartApi['addSeries']> | null>(null)
  const emaFastRef = useRef<ReturnType<IChartApi['addSeries']> | null>(null)
  const breakoutLineRef = useRef<ReturnType<IChartApi['addSeries']> | null>(null)
  const mode: Mode = 'upToEntry'

  const candles = useMemo(() => {
    if (!trade) return []
    const buf = mode === 'upToEntry' ? sliceUpToEntry(trade) : trade.chartBuffer
    return buf
      .filter((b) => b.open != null && b.high != null && b.low != null && b.close != null)
      .map((b) => ({
        time: toUnixSeconds(b.time) as UTCTimestamp,
        open: b.open as number,
        high: b.high as number,
        low: b.low as number,
        close: b.close as number,
      }))
  }, [trade, mode])

  const emaSlow = useMemo(() => {
    if (!trade) return []
    const buf = mode === 'upToEntry' ? sliceUpToEntry(trade) : trade.chartBuffer
    return buf
      .filter((b) => b.ema_slow != null)
      .map((b) => ({
        time: toUnixSeconds(b.time) as UTCTimestamp,
        value: b.ema_slow as number,
      }))
  }, [trade, mode])

  const emaMedium = useMemo(() => {
    if (!trade) return []
    const buf = mode === 'upToEntry' ? sliceUpToEntry(trade) : trade.chartBuffer
    return buf
      .filter((b) => b.ema_medium != null)
      .map((b) => ({
        time: toUnixSeconds(b.time) as UTCTimestamp,
        value: b.ema_medium as number,
      }))
  }, [trade, mode])

  const emaFast = useMemo(() => {
    if (!trade) return []
    const buf = mode === 'upToEntry' ? sliceUpToEntry(trade) : trade.chartBuffer
    return buf
      .filter((b) => b.ema_fast != null)
      .map((b) => ({
        time: toUnixSeconds(b.time) as UTCTimestamp,
        value: b.ema_fast as number,
      }))
  }, [trade, mode])

  const breakoutLineData = useMemo(() => {
    if (!trade) return []
    return [
      { time: toUnixSeconds(trade.setupTime) as UTCTimestamp, value: trade.entryPrice },
      { time: toUnixSeconds(trade.entryTime) as UTCTimestamp, value: trade.entryPrice },
    ]
  }, [trade])

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
        breakoutLineRef.current = null
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
      if (breakoutLineRef.current && breakoutLineData.length > 0) {
        breakoutLineRef.current.setData(breakoutLineData)
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

    // EMA colours: slow = black, medium = blue, fast = red (data from chartBuffer.ema_slow/ema_medium/ema_fast)
    const slowSeries = newChart.addSeries(LineSeries, {
      color: '#000000', // black — slow EMA
      lineWidth: 1,
      priceLineVisible: false,
      lastValueVisible: false,
    })
    const mediumSeries = newChart.addSeries(LineSeries, {
      color: '#1565c0', // blue — medium EMA
      lineWidth: 1,
      priceLineVisible: false,
      lastValueVisible: false,
    })
    const fastSeries = newChart.addSeries(LineSeries, {
      color: '#c62828', // red — fast EMA
      lineWidth: 1,
      priceLineVisible: false,
      lastValueVisible: false,
    })
    const breakoutSeries = newChart.addSeries(LineSeries, {
      color: '#c62828',
      lineWidth: 2,
      priceLineVisible: false,
      lastValueVisible: false,
    })
    emaSlowRef.current = slowSeries
    emaMediumRef.current = mediumSeries
    emaFastRef.current = fastSeries
    breakoutLineRef.current = breakoutSeries

    newSeries.setData(candles)
    slowSeries.setData(emaSlow)
    mediumSeries.setData(emaMedium)
    fastSeries.setData(emaFast)
    if (breakoutLineData.length > 0) {
      breakoutSeries.setData(breakoutLineData)
    }
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
      breakoutLineRef.current = null
    }
  }, [trade, candles, emaSlow, emaMedium, emaFast, breakoutLineData])

  if (!trade) {
    return (
      <div className="chart-placeholder">
        <h3>Chart TF</h3>
        <span>Select a trade to view chart</span>
      </div>
    )
  }

  const hasEmaData = emaSlow.length > 0 || emaMedium.length > 0 || emaFast.length > 0

  return (
    <div className="chart-panel">
      <div className="chart-panel-header">
        <h3>Chart TF (up to entry)</h3>
        <span className="chart-panel-subtitle">{trade.symbol} {trade.chartTF}</span>
        {!hasEmaData && (
          <span className="chart-panel-subtitle chart-panel-hint" title="Run scanner_indicators.py to populate EMAs in the DB">
            No EMA data — run pipeline: mtf_loader → scanner_indicators → scanner_entry_mgmt
          </span>
        )}
      </div>
      <div ref={containerRef} className="chart-canvas" />
    </div>
  )
}


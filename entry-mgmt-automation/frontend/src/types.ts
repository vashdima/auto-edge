export interface Run {
  run_id: number
  run_key: string | null
  scan_from: string
  scan_to: string
  created_at: string
}

export interface ChartBar {
  time: string
  open?: number
  high?: number
  low?: number
  close?: number
  ema_slow?: number
  ema_medium?: number
  ema_fast?: number
  atr?: number
}

export interface EntryMap {
  trade_id: number
  symbol: string
  chartTF: string
  contextTF: string
  validationTF: string
  run_id: number
  contextBullish: boolean
  validationOk: boolean
  state: string
  setupTime: string
  entryTime: string
  exitTime?: string
  entryDay: string
  entryPrice: number
  sl: number
  tp: number
  slSize: number | null
  beActive: boolean
  rr: number | null
  exitReason: string
  enrichScore: number | null
  chartBuffer: ChartBar[]
  contextBuffer: ChartBar[]
  validationBuffer: ChartBar[]
}

export interface RunStatsSummary {
  totalTrades: number
  wins: number
  losses: number
  breakevens: number
  winRate: number
  totalRR: number
  avgRR: number
  expectancy: number
}

export interface RunStatsDrawdown {
  maxDrawdownR: number
}

export interface RunStatsStreaks {
  maxWinningStreak: number
  maxLosingStreak: number
}

export interface HourlyEntryStat {
  hour: number
  trades: number
  wins: number
  losses: number
  breakevens: number
  winRate: number
  avgRR: number
  totalRR: number
}

export interface EquityPoint {
  index: number
  time: string
  rr: number
  equity: number
  drawdown: number
  exitReason: string
}

export interface RunStats {
  run_id: number
  summary: RunStatsSummary
  drawdown: RunStatsDrawdown
  streaks: RunStatsStreaks
  equityCurve: EquityPoint[]
  rrSeries: number[]
  hourlyByEntryUtc: HourlyEntryStat[]
}

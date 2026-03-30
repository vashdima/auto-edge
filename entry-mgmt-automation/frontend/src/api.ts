import type { EntryMap, Run, RunStats } from './types'

const API_BASE = import.meta.env.VITE_API_URL ?? 'http://localhost:8000'

export async function fetchRuns(): Promise<Run[]> {
  const res = await fetch(`${API_BASE}/runs`)
  if (!res.ok) throw new Error(`Failed to fetch runs: ${res.status}`)
  return res.json()
}

export async function fetchEntries(
  runId?: number,
  runKey?: string,
  options?: { summary?: boolean },
): Promise<EntryMap[]> {
  if (runId == null && runKey == null) {
    throw new Error('Provide runId or runKey')
  }
  const params = new URLSearchParams()
  if (runId != null) params.set('run_id', String(runId))
  if (runKey != null) params.set('run_key', runKey)
  if (options?.summary) params.set('summary', 'true')
  const res = await fetch(`${API_BASE}/entries?${params}`)
  if (!res.ok) throw new Error(`Failed to fetch entries: ${res.status}`)
  return res.json()
}

export interface TradeBuffersPatch {
  trade_id: number
  chartBuffer: EntryMap['chartBuffer']
  contextBuffer: EntryMap['contextBuffer']
  validationBuffer: EntryMap['validationBuffer']
  enrichScore: number | null
}

export async function fetchTradeBuffers(
  tradeIds: number[],
  runId?: number,
  runKey?: string,
): Promise<TradeBuffersPatch[]> {
  if (runId == null && runKey == null) {
    throw new Error('Provide runId or runKey')
  }
  if (tradeIds.length === 0) return []
  const params = new URLSearchParams()
  if (runId != null) params.set('run_id', String(runId))
  if (runKey != null) params.set('run_key', runKey)
  params.set('trade_ids', tradeIds.join(','))
  const res = await fetch(`${API_BASE}/trade-buffers?${params}`)
  if (!res.ok) throw new Error(`Failed to fetch trade buffers: ${res.status}`)
  return res.json()
}

export async function fetchRunStats(runId?: number, runKey?: string): Promise<RunStats> {
  if (runId == null && runKey == null) {
    throw new Error('Provide runId or runKey')
  }
  const params = new URLSearchParams()
  if (runId != null) params.set('run_id', String(runId))
  if (runKey != null) params.set('run_key', runKey)
  const res = await fetch(`${API_BASE}/run-stats?${params}`)
  if (!res.ok) throw new Error(`Failed to fetch run stats: ${res.status}`)
  return res.json()
}

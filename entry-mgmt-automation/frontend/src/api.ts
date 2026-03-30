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

export async function downloadRunConfig(run: Run): Promise<void> {
  if (run.run_id == null && run.run_key == null) {
    throw new Error('Invalid run')
  }
  const params = new URLSearchParams()
  if (run.run_key != null && run.run_key !== '') {
    params.set('run_key', run.run_key)
  } else {
    params.set('run_id', String(run.run_id))
  }
  const res = await fetch(`${API_BASE}/run-config?${params}`)
  if (!res.ok) {
    if (res.status === 404) {
      throw new Error('No config snapshot for this run')
    }
    throw new Error(`Failed to download config: ${res.status}`)
  }
  const text = await res.text()
  const blob = new Blob([text], { type: 'text/yaml;charset=utf-8' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = `config-run-${run.run_id}.yaml`
  document.body.appendChild(a)
  a.click()
  document.body.removeChild(a)
  URL.revokeObjectURL(url)
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

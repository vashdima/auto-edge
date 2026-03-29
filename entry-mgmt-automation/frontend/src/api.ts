import type { EntryMap, Run, RunStats } from './types'

const API_BASE = import.meta.env.VITE_API_URL ?? 'http://localhost:8000'

export async function fetchRuns(): Promise<Run[]> {
  const res = await fetch(`${API_BASE}/runs`)
  if (!res.ok) throw new Error(`Failed to fetch runs: ${res.status}`)
  return res.json()
}

export async function fetchEntries(runId?: number, runKey?: string): Promise<EntryMap[]> {
  if (runId == null && runKey == null) {
    throw new Error('Provide runId or runKey')
  }
  const params = new URLSearchParams()
  if (runId != null) params.set('run_id', String(runId))
  if (runKey != null) params.set('run_key', runKey)
  const res = await fetch(`${API_BASE}/entries?${params}`)
  if (!res.ok) throw new Error(`Failed to fetch entries: ${res.status}`)
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

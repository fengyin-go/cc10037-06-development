import { request } from './client'

const BASE = '/api/backfill-resume'

export type Blocker = {
  blocker_type: string
  source_table: string
  source_partition: string
  required_version: string
  confirmed: boolean
  detail: string
}

export type RequiredRead = {
  table: string
  partition: string
  read_version: string
  required_version: string
  available: boolean
  confirmed: boolean
}

export type ResumePartition = {
  partition: string
  latest_attempt_status: string
  category: string
  submittable: boolean
  current_result_version: string | null
  correction_version: string | null
  required_reads: RequiredRead[]
  confirmed_prerequisites: { table: string; partition: string; version: string; confirmed: boolean }[]
  blockers: Blocker[]
  receipt_state: string
}

export type ResumeView = {
  tracking_id: string
  target_table: string
  backfill_task: string
  generated_at: string
  view: {
    partitions: ResumePartition[]
    pending_partitions: string[]
    submittable_partitions: string[]
    blocked_partitions: string[]
    stable_partitions: string[]
    summary: Record<string, number>
  }
  receipts: {
    partition: string
    attempts: {
      attempt_no: number
      status: string
      result_version: string | null
      error_message: string
      blockers: Blocker[]
      write_position: Record<string, unknown>
    }[]
  }[]
  batches: ResumeBatch[]
}

export type ResumeBatch = {
  batch_id: number
  category: string
  state: string
  partitions: string[]
  partition_count: number
  coverage_start: string
  coverage_end: string
  read_positions: Record<string, RequiredRead[]>
  prerequisite_corrections: { table: string; partition: string; version: string }[]
  reason: string
}

export type DownstreamReadable = {
  partitions: {
    partition: string
    target_table: string
    current_result_version: string | null
    latest_confirmed_version: string | null
    downstream_can_read: boolean
    is_latest: boolean
    stale: boolean
    receipt_state: string
    readable_versions: string[]
  }[]
  actually_readable: string[]
  latest_and_confirmed: string[]
}

export type SubmitResult = {
  tracking_id: string
  submitted_batch: {
    state: string
    coverage_start: string
    coverage_end: string
    partitions: string[]
  }
  execution: {
    executed_at: string
    executed_partitions: {
      partition: string
      attempt_no: number
      result_version: string
      rows_written: number
      write_position: {
        resume_from: string
        upstream_reads: { table: string; partition: string; version: string }[]
      }
    }[]
  }
  next_view: ResumeView['view']
  next_batches: ResumeBatch[]
  readable: DownstreamReadable
}

async function getJson<T>(path: string): Promise<T> {
  const response = await request(path)
  if (!response.ok) throw new Error((await response.json()).detail ?? '接口请求失败')
  return (await response.json()) as T
}

async function postJson<T>(path: string): Promise<T> {
  const response = await request(path, { method: 'POST' })
  if (!response.ok) {
    let detail = `接口返回 ${response.status}`
    try {
      detail = (await response.json()).detail ?? detail
    } catch {
      /* 保留默认提示 */
    }
    throw new Error(detail)
  }
  return (await response.json()) as T
}

export function listResumeTrackers(): Promise<{ items: { tracking_id: string; target_table: string; partition_count: number }[] }> {
  return getJson(`${BASE}/trackers`)
}

export function fetchResumeView(trackingId: string): Promise<ResumeView> {
  return getJson(`${BASE}/${trackingId}/view`)
}

export function cutResumeBatches(trackingId: string): Promise<{ batches: ResumeBatch[] }> {
  return postJson(`${BASE}/${trackingId}/batches/cut`)
}

export function submitResumeBatch(trackingId: string, batchId: number): Promise<SubmitResult> {
  return postJson(`${BASE}/${trackingId}/batches/${batchId}/submit`)
}

export function fetchDownstreamReadable(trackingId: string): Promise<DownstreamReadable> {
  return getJson(`${BASE}/${trackingId}/downstream-readable`)
}

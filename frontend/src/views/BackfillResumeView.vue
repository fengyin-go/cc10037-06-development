<template>
  <section class="page" data-module="backfill-resume">
    <header class="page-head">
      <div>
        <h2>回填续跑工作台</h2>
        <p class="page-desc">
          按分区写入回执关联每次尝试的依赖阻塞明细，区分可独立恢复的失败日与等待上游更正的依赖组
        </p>
      </div>
      <div class="page-actions">
        <label class="tracker-picker">
          <span>回填追踪</span>
          <select v-model="trackingId" data-testid="tracker-picker" @change="reload">
            <option v-for="item in trackers" :key="item.tracking_id" :value="item.tracking_id">
              {{ item.tracking_id }}（{{ item.partition_count }}个分区）
            </option>
          </select>
        </label>
        <button class="btn primary" data-testid="cut-batches" type="button" :disabled="loading" @click="cutBatches">
          按阻塞明细切批
        </button>
      </div>
    </header>

    <div v-if="view" class="stat-row">
      <article class="stat-card">
        <span class="stat-label">分区总数</span>
        <strong class="stat-value">{{ view.view.summary.total }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">待恢复</span>
        <strong class="stat-value">{{ view.view.summary.pending }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">可立即提交</span>
        <strong class="stat-value">{{ view.view.summary.submittable }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">依赖阻塞</span>
        <strong class="stat-value">{{ view.view.summary.blocked }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">已稳定</span>
        <strong class="stat-value">{{ view.view.summary.stable }}</strong>
      </article>
    </div>

    <p v-if="view" class="status-legend">
      <span class="legend-item">目标表：{{ view.target_table }}</span>
      <span class="legend-item">回填任务：{{ view.backfill_task }}</span>
      <span class="legend-item">视图生成：{{ view.generated_at }}</span>
    </p>

    <h3 class="section-title">续跑分区与依赖阻塞明细</h3>
    <table class="data-table" data-testid="resume-table">
      <thead>
        <tr>
          <th>日分区</th>
          <th>最近尝试</th>
          <th>续跑分类</th>
          <th>当前/更正版本</th>
          <th>依赖阻塞明细</th>
          <th>确认过的前置结果</th>
          <th>回执状态</th>
          <th>可提交</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in view?.view.partitions ?? []" :key="row.partition" :data-category="row.category">
          <td>{{ row.partition }}</td>
          <td>{{ row.latest_attempt_status }}</td>
          <td>
            <span class="tag" :class="categoryClass(row.category)">{{ row.category }}</span>
          </td>
          <td>
            <div>当前：{{ row.current_result_version ?? '—' }}</div>
            <div v-if="row.correction_version" class="correction-text">
              更正版本：{{ row.correction_version }}
            </div>
          </td>
          <td class="blocker-cell">
            <div v-if="!row.blockers.length" class="muted">无阻塞</div>
            <div v-for="(blocker, idx) in row.blockers" :key="idx" class="blocker-line">
              <strong>{{ blocker.blocker_type }}</strong>
              <span>{{ blocker.source_table }} / {{ blocker.source_partition }}</span>
              <span>需要版本：{{ blocker.required_version }}（{{ blocker.confirmed ? '已确认' : '未确认' }}）</span>
              <span class="muted">{{ blocker.detail }}</span>
            </div>
          </td>
          <td>
            <div v-if="!row.confirmed_prerequisites.length" class="muted">—</div>
            <div v-for="(pre, idx) in row.confirmed_prerequisites" :key="idx">
              {{ pre.table }} / {{ pre.partition }} @ {{ pre.version }}
            </div>
          </td>
          <td><span class="tag" :class="stateClass(row.receipt_state)">{{ row.receipt_state }}</span></td>
          <td>
            <span :class="row.submittable ? 'ok-text' : 'muted'">
              {{ row.submittable ? '是' : '否' }}
            </span>
          </td>
        </tr>
      </tbody>
    </table>

    <h3 class="section-title">写入尝试与回执（按分区）</h3>
    <div class="receipt-grid">
      <article v-for="receipt in view?.receipts ?? []" :key="receipt.partition" class="receipt-card">
        <header>
          <strong>{{ receipt.partition }}</strong>
          <span class="muted">尝试 {{ receipt.attempts.length }} 次</span>
        </header>
        <ol class="attempt-list">
          <li v-for="attempt in receipt.attempts" :key="attempt.attempt_no">
            <div>
              第{{ attempt.attempt_no }}次 ·
              <span :class="attempt.status === '成功' ? 'ok-text' : 'error-text'">{{ attempt.status }}</span>
              <template v-if="attempt.result_version"> · 产出 {{ attempt.result_version }}</template>
            </div>
            <div v-if="attempt.error_message" class="error-text">{{ attempt.error_message }}</div>
            <div v-for="(blocker, idx) in attempt.blockers" :key="idx" class="muted blocker-detail">
              {{ blocker.blocker_type }}：{{ blocker.detail }}
            </div>
          </li>
        </ol>
      </article>
    </div>

    <h3 class="section-title">续跑批次（按依赖阻塞明细切分）</h3>
    <table class="data-table" data-testid="batch-table">
      <thead>
        <tr>
          <th>批次</th>
          <th>分类</th>
          <th>状态</th>
          <th>覆盖日期</th>
          <th>重新取数位置</th>
          <th>前置更正</th>
          <th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="batch in view?.batches ?? []" :key="batch.batch_id" :data-state="batch.state">
          <td>#{{ batch.batch_id }}</td>
          <td><span class="tag" :class="categoryClass(batch.category)">{{ batch.category }}</span></td>
          <td><span class="tag" :class="batch.state === '可提交' ? 'tag-ok' : 'tag-blocked'">{{ batch.state }}</span></td>
          <td>{{ batch.coverage_start }} ~ {{ batch.coverage_end }}（{{ batch.partition_count }}个分区）</td>
          <td class="position-cell">
            <div v-for="(reads, partition) in batch.read_positions" :key="partition">
              <strong>{{ partition }}：</strong>
              <span v-for="read in reads" :key="read.table + read.partition">
                {{ read.table }}/{{ read.partition }} → {{ read.required_version }}
              </span>
            </div>
          </td>
          <td>
            <div v-if="!batch.prerequisite_corrections.length" class="muted">无</div>
            <div v-for="(pre, idx) in batch.prerequisite_corrections" :key="idx">
              {{ pre.table }}/{{ pre.partition }} @ {{ pre.version }}
            </div>
          </td>
          <td>
            <button
              class="btn primary"
              type="button"
              :data-testid="`submit-batch-${batch.batch_id}`"
              :disabled="batch.state !== '可提交' || submitting"
              @click="submitBatch(batch.batch_id)"
            >
              提交回刷
            </button>
            <span v-if="batch.state !== '可提交'" class="muted submit-hint">等待依赖组解锁</span>
          </td>
        </tr>
      </tbody>
    </table>

    <h3 class="section-title">下游实际可读取分区核对</h3>
    <table class="data-table" data-testid="readable-table">
      <thead>
        <tr>
          <th>分区</th>
          <th>当前版本</th>
          <th>最新确认版本</th>
          <th>下游可读</th>
          <th>是否最新</th>
          <th>状态</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in readable?.partitions ?? []" :key="row.partition">
          <td>{{ row.partition }}</td>
          <td>{{ row.current_result_version ?? '—' }}</td>
          <td>{{ row.latest_confirmed_version ?? '—' }}</td>
          <td>{{ row.downstream_can_read ? '可读' : '不可读' }}</td>
          <td>
            <span :class="row.is_latest && !row.stale ? 'ok-text' : 'error-text'">
              {{ row.is_latest && !row.stale ? '是' : '否' }}
            </span>
          </td>
          <td><span class="tag" :class="stateClass(row.receipt_state)">{{ row.receipt_state }}</span></td>
        </tr>
      </tbody>
    </table>

    <footer class="page-foot">
      <span v-if="readable">
        可读取 {{ readable.actually_readable.length }} 个分区，
        其中最新确认 {{ readable.latest_and_confirmed.length }} 个
      </span>
      <span v-if="errorMessage" class="error-text" data-testid="resume-error">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import {
  cutResumeBatches,
  fetchDownstreamReadable,
  fetchResumeView,
  listResumeTrackers,
  submitResumeBatch,
} from '@/api/backfill-resume'
import type { DownstreamReadable, ResumeBatch, ResumeView } from '@/api/backfill-resume'

const trackers = ref<{ tracking_id: string; target_table: string; partition_count: number }[]>([])
const trackingId = ref('')
const view = ref<ResumeView | null>(null)
const readable = ref<DownstreamReadable | null>(null)
const loading = ref(false)
const submitting = ref(false)
const errorMessage = ref('')

function categoryClass(category: string): string {
  if (category === '独立失败日') return 'tag-independent'
  if (category === '更正续跑组') return 'tag-correction'
  if (category === '依赖等待组') return 'tag-blocked'
  return 'tag-stable'
}

function stateClass(state: string): string {
  if (state === '当前可读') return 'tag-ok'
  if (state === '已被更正取代') return 'tag-correction'
  if (state === '上游未满足不可读') return 'tag-blocked'
  return 'tag-stable'
}

async function reload() {
  if (!trackingId.value) return
  loading.value = true
  errorMessage.value = ''
  try {
    view.value = await fetchResumeView(trackingId.value)
    readable.value = await fetchDownstreamReadable(trackingId.value)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '续跑视图读取失败'
  } finally {
    loading.value = false
  }
}

async function cutBatches() {
  if (!trackingId.value) return
  errorMessage.value = ''
  try {
    const payload = await cutResumeBatches(trackingId.value)
    if (view.value) {
      view.value = { ...view.value, batches: payload.batches as ResumeBatch[] }
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '批次切分失败'
  }
}

async function submitBatch(batchId: number) {
  if (!trackingId.value) return
  submitting.value = true
  errorMessage.value = ''
  try {
    const result = await submitResumeBatch(trackingId.value, batchId)
    if (view.value) {
      view.value = {
        ...view.value,
        view: result.next_view,
        batches: result.next_batches,
      }
    }
    readable.value = result.readable
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '批次提交失败'
  } finally {
    submitting.value = false
  }
}

onMounted(async () => {
  try {
    const payload = await listResumeTrackers()
    trackers.value = payload.items
    trackingId.value = payload.items[0]?.tracking_id ?? ''
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '回填追踪读取失败'
  }
})
</script>

<style scoped>
.tracker-picker {
  display: flex;
  flex-direction: column;
  gap: 4px;
  font-size: 12px;
  color: var(--muted);
}
.tracker-picker select {
  padding: 6px 10px;
  border: 1px solid var(--border);
  border-radius: 6px;
  font-size: 13px;
}
.section-title {
  font-size: 15px;
  margin: 20px 0 8px;
}
.tag {
  display: inline-block;
  padding: 2px 8px;
  border-radius: 10px;
  font-size: 12px;
  white-space: nowrap;
}
.tag-independent { background: #fef3c7; color: #92400e; }
.tag-correction { background: #dbeafe; color: #1e40af; }
.tag-blocked { background: #fee2e2; color: #991b1b; }
.tag-stable { background: #dcfce7; color: #166534; }
.tag-ok { background: #dcfce7; color: #166534; }
.correction-text { color: #1d4ed8; font-size: 12px; }
.muted { color: var(--muted); font-size: 12px; }
.ok-text { color: #15803d; }
.error-text { color: #b42318; }
.blocker-cell { min-width: 320px; }
.blocker-line { display: flex; flex-direction: column; gap: 2px; padding: 4px 0; border-bottom: 1px dashed #e5e7eb; }
.receipt-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 12px; }
.receipt-card {
  background: #fff;
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 12px;
}
.receipt-card header { display: flex; justify-content: space-between; margin-bottom: 8px; }
.attempt-list { margin: 0; padding-left: 18px; font-size: 13px; }
.attempt-list li { margin-bottom: 8px; }
.blocker-detail { margin-top: 2px; }
.position-cell div { margin-bottom: 4px; font-size: 12px; }
.position-cell span { margin-right: 8px; }
.submit-hint { margin-left: 8px; }
</style>

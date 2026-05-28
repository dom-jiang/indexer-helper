<template>
  <div v-loading="loading">
    <el-page-header @back="$router.push('/dashboard')" :title="'Back'" :content="token?.app_name || 'API Key Detail'" />

    <el-row :gutter="20" style="margin-top: 24px" v-if="token">
      <el-col :span="12">
        <el-card>
          <template #header><strong>Key Information</strong></template>
          <el-descriptions :column="1" border>
            <el-descriptions-item label="App Name">{{ token.app_name || '—' }}</el-descriptions-item>
            <el-descriptions-item label="App ID">
              <span class="mono">{{ token.app_id }}</span>
            </el-descriptions-item>
            <el-descriptions-item label="Recipient">
              <span class="mono">{{ token.refund_address || '—' }}</span>
            </el-descriptions-item>
            <el-descriptions-item label="App Fee">
              {{ token.app_fee ? `${token.app_fee}%` : 'Not set' }}
            </el-descriptions-item>
            <el-descriptions-item label="Status">
              <el-tag :type="token.status === 1 ? 'success' : 'danger'" size="small">
                {{ token.status === 1 ? 'Active' : 'Disabled' }}
              </el-tag>
            </el-descriptions-item>
            <el-descriptions-item label="JWT issued">
              {{ token.swap_jwt_issued_at || '—' }}
            </el-descriptions-item>
            <el-descriptions-item label="JWT issues">
              {{ jwtIssueCount }} / {{ jwtIssueLimit }} used
              <span v-if="jwtIssuesRemaining === 0" class="warn-text"> (limit reached)</span>
            </el-descriptions-item>
            <el-descriptions-item label="Created">{{ token.created_at }}</el-descriptions-item>
          </el-descriptions>

          <el-alert
            v-if="!auth.isUserActive"
            type="error"
            :closable="false"
            title="Your account has been disabled."
            style="margin-top: 12px"
          />
          <el-alert
            v-else-if="token.status !== 1"
            type="warning"
            :closable="false"
            title="This API key is disabled. Swap API requests will be rejected."
            style="margin-top: 12px"
          />

          <div style="margin-top: 16px">
            <h4 style="margin-bottom: 8px">API JWT</h4>
            <el-input
              v-if="activeJwt"
              type="textarea"
              :model-value="activeJwt"
              :rows="5"
              readonly
              class="mono"
            />
            <el-empty v-else description="No JWT stored. Regenerate to create one." :image-size="48" />
            <div style="margin-top: 12px; display: flex; gap: 8px; flex-wrap: wrap">
              <el-button type="primary" :disabled="!activeJwt" @click="copyText(activeJwt)">
                Copy JWT
              </el-button>
              <el-popconfirm
                :title="regenerateConfirmTitle"
                @confirm="regenerateJwt"
              >
                <template #reference>
                  <el-button type="warning" :disabled="!canRegenerateJwt">Regenerate JWT</el-button>
                </template>
              </el-popconfirm>
              <el-button :disabled="!canManageKey" @click="openEditDialog">Edit Settings</el-button>
            </div>
            <p class="hint">
              JWT does not expire by time. Each API key may issue a JWT at most {{ jwtIssueLimit }} times
              (including creation). Regenerating invalidates the previous token.
            </p>
          </div>
        </el-card>
      </el-col>

      <el-col :span="12">
        <el-card>
          <template #header><strong>Usage Statistics</strong></template>
          <el-descriptions :column="1" border v-if="usage">
            <el-descriptions-item label="Quote this minute">
              {{ usage.usage.quote_this_minute }} / {{ effectiveLimits.quote.per_minute }}
            </el-descriptions-item>
            <el-descriptions-item label="Build this minute">
              {{ usage.usage.build_this_minute }} / {{ effectiveLimits.build.per_minute }}
            </el-descriptions-item>
            <el-descriptions-item label="Total this month">
              {{ usage.usage.total_this_month }}
            </el-descriptions-item>
          </el-descriptions>
          <el-divider />
          <h4 style="margin-bottom: 12px">Rate Limits</h4>
          <el-table :data="rateLimitList" size="small" border>
            <el-table-column prop="endpoint_group" label="Endpoint" width="100" />
            <el-table-column prop="per_minute" label="Per Minute" width="100" />
            <el-table-column prop="per_month" label="Per Month" />
          </el-table>
        </el-card>
      </el-col>
    </el-row>

    <el-dialog v-model="showEdit" title="Edit Settings" width="480px">
      <el-form ref="editFormRef" :model="editForm" :rules="editRules" label-position="top">
        <el-form-item label="Recipient" prop="refundAddress">
          <el-input v-model="editForm.refundAddress" placeholder="0x... (recipient wallet address)" />
        </el-form-item>
        <el-form-item label="App Fee (%)">
          <el-input-number
            v-model="editForm.appFee"
            :min="0"
            :max="10"
            :step="0.01"
            :precision="2"
            controls-position="right"
            style="width: 100%"
          />
          <div class="form-tip">Set between 0% ~ 10%. Use 0 to disable.</div>
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="showEdit = false">Cancel</el-button>
        <el-button type="primary" :loading="saving" @click="saveSettings">Save</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { useRoute } from 'vue-router'
import { ElMessage } from 'element-plus'
import api from '../api'
import { useAuthStore } from '../store'

const route = useRoute()
const auth = useAuthStore()
const tokenId = computed(() => route.params.id)

const editFormRef = ref(null)
const editRules = {
  refundAddress: [
    { required: true, message: 'Recipient is required', trigger: 'blur' },
    { min: 1, message: 'Recipient is required', trigger: 'blur' },
  ],
}

const token = ref(null)
const usage = ref(null)
const loading = ref(false)
const showEdit = ref(false)
const saving = ref(false)
const editForm = ref({ refundAddress: '', appFee: 0 })

const effectiveLimits = computed(() => {
  const fromUsage = usage.value?.limits
  if (fromUsage?.quote && fromUsage?.build) return fromUsage
  const rows = token.value?.rate_limits || []
  const quote = rows.find(r => r.endpoint_group === 'quote')
  const build = rows.find(r => r.endpoint_group === 'build')
  return {
    quote: { per_minute: quote?.per_minute ?? 60, per_month: quote?.per_month ?? 300000 },
    build: { per_minute: build?.per_minute ?? 30, per_month: build?.per_month ?? 300000 },
  }
})

const rateLimitList = computed(() => [
  { endpoint_group: 'quote', ...effectiveLimits.value.quote },
  { endpoint_group: 'build', ...effectiveLimits.value.build },
])
const canManageKey = computed(() => auth.isUserActive && token.value?.status === 1)
const activeJwt = computed(() => token.value?.swap_jwt || token.value?.jwt || '')
const jwtIssueLimit = computed(() => Number(token.value?.swap_jwt_issue_limit) || 3)
const jwtIssueCount = computed(() => Number(token.value?.swap_jwt_issue_count) || 0)
const jwtIssuesRemaining = computed(
  () => Number(token.value?.swap_jwt_issues_remaining ?? Math.max(0, jwtIssueLimit.value - jwtIssueCount.value))
)
const canRegenerateJwt = computed(
  () => canManageKey.value && jwtIssuesRemaining.value > 0
)
const regenerateConfirmTitle = computed(() =>
  jwtIssuesRemaining.value <= 1
    ? 'Generate a new JWT? This is your last allowed issue; the current JWT will stop working.'
    : 'Generate a new JWT? The current JWT will stop working immediately.'
)

async function fetchDetail() {
  loading.value = true
  try {
    const [detailRes, usageRes] = await Promise.all([
      api.get(`/api-tokens/${tokenId.value}`),
      api.get(`/api-tokens/${tokenId.value}/usage`),
    ])
    if (detailRes.code === 0) token.value = detailRes.data
    if (usageRes.code === 0) usage.value = usageRes.data
  } finally {
    loading.value = false
  }
}

function openEditDialog() {
  editForm.value = {
    refundAddress: token.value?.refund_address || '',
    appFee: parseFloat(token.value?.app_fee) || 0,
  }
  showEdit.value = true
}

async function saveSettings() {
  if (!canManageKey.value) {
    ElMessage.error(token.value?.status !== 1 ? 'This API key is disabled' : 'Your account is disabled')
    return
  }
  const valid = await editFormRef.value?.validate().catch(() => false)
  if (!valid) return
  const fee = Number(editForm.value.appFee)
  if (Number.isNaN(fee) || fee < 0 || fee > 10) {
    ElMessage.warning('App Fee must be between 0% and 10%')
    return
  }
  saving.value = true
  try {
    const res = await api.put(`/api-tokens/${tokenId.value}`, {
      refundAddress: editForm.value.refundAddress,
      appFee: editForm.value.appFee,
    })
    if (res.code === 0) {
      ElMessage.success('Settings saved')
      showEdit.value = false
      fetchDetail()
    } else {
      ElMessage.error(res.msg || 'Save failed')
    }
  } catch {
    ElMessage.error('Save failed')
  } finally {
    saving.value = false
  }
}

async function regenerateJwt() {
  if (!canRegenerateJwt.value) {
    if (!canManageKey.value) {
      ElMessage.error(token.value?.status !== 1 ? 'This API key is disabled' : 'Your account is disabled')
    } else {
      ElMessage.warning(`JWT issue limit reached (${jwtIssueLimit.value} times per API key)`)
    }
    return
  }
  try {
    const res = await api.post(`/api-tokens/${tokenId.value}/generate-jwt`, {})
    if (res.code === 0) {
      ElMessage.success('JWT regenerated')
      token.value = res.data || token.value
    } else {
      ElMessage.error(res.msg || 'Failed to regenerate JWT')
    }
  } catch {
    ElMessage.error('Failed to regenerate JWT')
  }
}

function copyText(text) {
  if (!text) return
  navigator.clipboard.writeText(text)
  ElMessage.success('Copied')
}

onMounted(fetchDetail)
</script>

<style scoped>
.mono {
  font-family: 'Courier New', monospace;
  font-size: 13px;
  word-break: break-all;
}
.form-tip,
.hint {
  font-size: 12px;
  color: #909399;
  margin-top: 8px;
}
.warn-text {
  color: #e6a23c;
  font-size: 12px;
}
</style>

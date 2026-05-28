<template>
  <div>
    <el-alert
      v-if="!auth.isUserActive"
      type="error"
      :closable="false"
      title="Your account has been disabled. You cannot create API keys or use JWT tokens."
      style="margin-bottom: 16px"
    />

    <div class="page-header">
      <h2>My API Key</h2>
      <el-button
        v-if="!hasKey"
        type="primary"
        :disabled="!auth.isUserActive"
        @click="showCreate = true"
      >Create API Key</el-button>
    </div>

    <el-alert
      v-if="hasKey"
      type="info"
      :closable="false"
      style="margin-top: 12px"
      title="Each account has one API key and one active JWT (max 3 issues per key). Regenerating invalidates the previous token."
    />

    <el-table :data="tokens" v-loading="loading" stripe style="width: 100%; margin-top: 16px">
      <el-table-column prop="app_name" label="App Name" min-width="120">
        <template #default="{ row }">{{ row.app_name || '—' }}</template>
      </el-table-column>
      <el-table-column prop="app_id" label="App ID" min-width="180">
        <template #default="{ row }">
          <span class="mono">{{ row.app_id }}</span>
        </template>
      </el-table-column>
      <el-table-column label="JWT" min-width="140">
        <template #default="{ row }">
          <span v-if="row.swap_jwt || row.jwt" class="jwt-hint">
            Active ({{ row.swap_jwt_issue_count || 0 }}/{{ row.swap_jwt_issue_limit || 3 }})
          </span>
          <span v-else class="jwt-hint muted">Not issued</span>
        </template>
      </el-table-column>
      <el-table-column prop="status" label="Status" width="90">
        <template #default="{ row }">
          <el-tag :type="row.status === 1 ? 'success' : 'danger'" size="small">
            {{ row.status === 1 ? 'Active' : 'Disabled' }}
          </el-tag>
        </template>
      </el-table-column>
      <el-table-column label="Actions" width="260">
        <template #default="{ row }">
          <el-button text type="primary" size="small" @click="viewJwt(row)">View JWT</el-button>
          <el-popconfirm
            :title="regenerateTitle(row)"
            @confirm="regenerateJwt(row)"
          >
            <template #reference>
              <el-button
                text
                type="warning"
                size="small"
                :disabled="!canRegenerateRow(row)"
              >Regenerate</el-button>
            </template>
          </el-popconfirm>
          <el-button text type="primary" size="small" @click="$router.push(`/tokens/${row.id}`)">
            Detail
          </el-button>
        </template>
      </el-table-column>
    </el-table>

    <el-card v-if="hasKey && displayJwt" style="margin-top: 20px">
      <template #header><strong>API JWT</strong></template>
      <el-input type="textarea" :model-value="displayJwt" :rows="5" readonly class="mono" />
      <div style="margin-top: 12px; display: flex; gap: 8px">
        <el-button type="primary" @click="copyText(displayJwt)">Copy JWT</el-button>
        <span v-if="jwtIssuedAt" class="meta">Issued at: {{ jwtIssuedAt }}</span>
      </div>
    </el-card>

    <el-dialog v-model="showCreate" title="Create API Key" width="480px">
      <el-alert type="info" :closable="false" style="margin-bottom: 12px">
        One API key per account. The first JWT counts as issue 1 of 3 (no expiry).
      </el-alert>
      <el-form ref="createFormRef" :model="newForm" :rules="createRules" label-position="top">
        <el-form-item label="App Name">
          <el-input v-model="newForm.appName" placeholder="My Application" />
        </el-form-item>
        <el-form-item label="Recipient" prop="refundAddress">
          <el-input v-model="newForm.refundAddress" placeholder="0x... (recipient wallet address)" />
        </el-form-item>
        <el-form-item label="App Fee (%)">
          <el-input-number
            v-model="newForm.appFee"
            :min="0"
            :max="10"
            :step="0.5"
            :precision="2"
            controls-position="right"
            style="width: 100%"
          />
          <div class="form-tip">Set between 1% ~ 10%. Leave 0 to disable.</div>
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="showCreate = false">Cancel</el-button>
        <el-button type="primary" :loading="creating" @click="createToken">Create</el-button>
      </template>
    </el-dialog>

    <el-dialog v-model="showJwt" title="API JWT" width="620px">
      <el-input type="textarea" :model-value="jwtToken" :rows="6" readonly />
      <template #footer>
        <el-button type="primary" @click="copyText(jwtToken)">Copy</el-button>
        <el-button @click="showJwt = false">Close</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { ElMessage } from 'element-plus'
import api from '../api'
import { useAuthStore } from '../store'

const auth = useAuthStore()
const createFormRef = ref(null)
const createRules = {
  refundAddress: [
    { required: true, message: 'Recipient is required', trigger: 'blur' },
    { min: 1, message: 'Recipient is required', trigger: 'blur' },
  ],
}

const tokens = ref([])
const loading = ref(false)
const showCreate = ref(false)
const creating = ref(false)
const newForm = ref({ appName: '', refundAddress: '', appFee: 0 })
const showJwt = ref(false)
const jwtToken = ref('')

const hasKey = computed(() => tokens.value.length > 0)
const primaryKey = computed(() => tokens.value[0] || null)
const displayJwt = computed(
  () => primaryKey.value?.swap_jwt || primaryKey.value?.jwt || ''
)
const jwtIssuedAt = computed(() => primaryKey.value?.swap_jwt_issued_at || '')

async function fetchTokens() {
  loading.value = true
  try {
    const res = await api.get('/api-tokens')
    if (res.code === 0) tokens.value = res.data || []
  } finally {
    loading.value = false
  }
}

function viewJwt(row) {
  const jwt = row.swap_jwt || row.jwt || ''
  if (!jwt) {
    ElMessage.warning('No JWT yet. Use Regenerate to create one.')
    return
  }
  jwtToken.value = jwt
  showJwt.value = true
}

async function createToken() {
  if (!auth.isUserActive) {
    ElMessage.error('Your account is disabled')
    return
  }
  const valid = await createFormRef.value?.validate().catch(() => false)
  if (!valid) return
  if (newForm.value.appFee !== 0 && (newForm.value.appFee < 1 || newForm.value.appFee > 10)) {
    ElMessage.warning('App Fee must be between 1% and 10%, or 0 to disable')
    return
  }
  creating.value = true
  try {
    const res = await api.post('/api-tokens', {
      appName: newForm.value.appName,
      refundAddress: newForm.value.refundAddress,
      appFee: newForm.value.appFee,
    })
    if (res.code === 0) {
      ElMessage.success('API Key and JWT created')
      showCreate.value = false
      newForm.value = { appName: '', refundAddress: '', appFee: 0 }
      await fetchTokens()
      if (res.data?.jwt) {
        jwtToken.value = res.data.jwt
        showJwt.value = true
      }
    } else {
      ElMessage.error(res.msg)
    }
  } finally {
    creating.value = false
  }
}

function jwtLimit(row) {
  return Number(row?.swap_jwt_issue_limit) || 3
}

function jwtUsed(row) {
  return Number(row?.swap_jwt_issue_count) || 0
}

function jwtRemaining(row) {
  const r = row?.swap_jwt_issues_remaining
  if (r !== undefined && r !== null) return Number(r)
  return Math.max(0, jwtLimit(row) - jwtUsed(row))
}

function canRegenerateRow(row) {
  return auth.isUserActive && row.status === 1 && jwtRemaining(row) > 0
}

function regenerateTitle(row) {
  const left = jwtRemaining(row)
  if (left <= 1) {
    return 'Generate a new JWT? This is your last allowed issue for this API key.'
  }
  return 'Generate a new JWT? The current JWT will stop working immediately.'
}

async function regenerateJwt(row) {
  if (!canRegenerateRow(row)) {
    if (!auth.isUserActive) {
      ElMessage.error('Your account is disabled')
    } else if (row.status !== 1) {
      ElMessage.error('This API key is disabled')
    } else {
      ElMessage.warning(`JWT issue limit reached (${jwtLimit(row)} per API key)`)
    }
    return
  }
  try {
    const res = await api.post(`/api-tokens/${row.id}/generate-jwt`, {})
    if (res.code === 0) {
      ElMessage.success('JWT regenerated. Previous JWT is now invalid.')
      await fetchTokens()
      const jwt = res.data?.jwt || res.data?.swap_jwt || ''
      if (jwt) {
        jwtToken.value = jwt
        showJwt.value = true
      }
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
  ElMessage.success('Copied to clipboard')
}

onMounted(fetchTokens)
</script>

<style scoped>
.page-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
}
.mono {
  font-family: 'Courier New', monospace;
  font-size: 13px;
}
.form-tip {
  font-size: 12px;
  color: #909399;
  margin-top: 4px;
}
.jwt-hint {
  font-size: 13px;
}
.jwt-hint.muted {
  color: #909399;
}
.meta {
  font-size: 12px;
  color: #909399;
  line-height: 32px;
}
</style>

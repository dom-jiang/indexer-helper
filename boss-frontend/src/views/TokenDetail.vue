<template>
  <div v-loading="loading">
    <el-page-header @back="$router.push('/dashboard')" :title="'Back'" :content="token?.app_name || 'Token Detail'" />

    <el-row :gutter="20" style="margin-top: 24px" v-if="token">
      <el-col :span="12">
        <el-card>
          <template #header><strong>Key Information</strong></template>
          <el-descriptions :column="1" border>
            <el-descriptions-item label="App Name">{{ token.app_name || '—' }}</el-descriptions-item>
            <el-descriptions-item label="App ID"><span class="mono">{{ token.app_id }}</span></el-descriptions-item>
            <el-descriptions-item label="Refund Address">
              <span class="mono">{{ token.refund_address || '—' }}</span>
            </el-descriptions-item>
            <el-descriptions-item label="App Fee">
              {{ token.app_fee ? `${token.app_fee}%` : 'Not set' }}
            </el-descriptions-item>
            <el-descriptions-item label="Status">
              <el-tag :type="token.status === 1 ? 'success' : 'danger'" size="small">{{ token.status === 1 ? 'Active' : 'Disabled' }}</el-tag>
            </el-descriptions-item>
            <el-descriptions-item label="Created">{{ token.created_at }}</el-descriptions-item>
          </el-descriptions>
          <div style="margin-top: 16px; display: flex; gap: 8px">
            <el-button type="primary" @click="generateJwt">Generate JWT Token</el-button>
            <el-button @click="openEditDialog">Edit Settings</el-button>
            <el-popconfirm title="This will invalidate all existing JWT tokens for this key. Continue?" @confirm="resetSecret">
              <template #reference>
                <el-button type="warning">Reset Secret</el-button>
              </template>
            </el-popconfirm>
          </div>
        </el-card>
      </el-col>

      <el-col :span="12">
        <el-card>
          <template #header><strong>Usage Statistics</strong></template>
          <el-descriptions :column="1" border v-if="usage">
            <el-descriptions-item label="Quote this minute">{{ usage.usage.quote_this_minute }}</el-descriptions-item>
            <el-descriptions-item label="Build this minute">{{ usage.usage.build_this_minute }}</el-descriptions-item>
            <el-descriptions-item label="Total this month">{{ usage.usage.total_this_month }}</el-descriptions-item>
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
      <el-form label-position="top">
        <el-form-item label="Refund Address">
          <el-input v-model="editForm.refundAddress" placeholder="0x... (wallet address for refunds)" />
        </el-form-item>
        <el-form-item label="App Fee (%)">
          <el-input-number
            v-model="editForm.appFee"
            :min="0" :max="10" :step="0.5" :precision="2"
            controls-position="right"
            style="width: 100%"
          />
          <div style="font-size: 12px; color: #909399; margin-top: 4px;">Set between 1% ~ 10%. Leave 0 to disable.</div>
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="showEdit = false">Cancel</el-button>
        <el-button type="primary" :loading="saving" @click="saveSettings">Save</el-button>
      </template>
    </el-dialog>

    <el-dialog v-model="showJwt" title="Generated JWT Token" width="620px">
      <el-alert type="warning" :closable="false" style="margin-bottom: 12px">
        This JWT is valid for 30 days. Store it securely — it will not be shown again.
      </el-alert>
      <el-input type="textarea" :model-value="jwtToken" :rows="6" readonly />
      <div style="margin-top: 12px; color: #909399; font-size: 13px;">
        Use this token in your API requests:<br/>
        <code style="background: #f5f7fa; padding: 2px 6px; border-radius: 3px;">Authorization: Bearer &lt;your_jwt_token&gt;</code>
      </div>
      <template #footer>
        <el-button @click="copyText(jwtToken); showJwt = false" type="primary">Copy & Close</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import api from '../api'

const route = useRoute()
const router = useRouter()
const tokenId = computed(() => route.params.id)

const token = ref(null)
const usage = ref(null)
const loading = ref(false)
const showJwt = ref(false)
const jwtToken = ref('')
const showEdit = ref(false)
const saving = ref(false)
const editForm = ref({ refundAddress: '', appFee: 0 })

const rateLimitList = computed(() => token.value?.rate_limits || [])

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
  const fee = editForm.value.appFee
  if (fee !== 0 && (fee < 1 || fee > 10)) {
    ElMessage.warning('App Fee must be between 1% and 10%, or 0 to disable')
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

async function resetSecret() {
  try {
    const res = await api.post(`/api-tokens/${tokenId.value}/reset-key`)
    if (res.code === 0) {
      ElMessage.success('Secret reset successfully. All existing JWT tokens have been invalidated.')
      fetchDetail()
    }
  } catch {
    ElMessage.error('Reset failed')
  }
}

async function generateJwt() {
  try {
    const res = await api.post(`/api-tokens/${tokenId.value}/generate-jwt`, { expiresIn: 86400 * 30 })
    if (res.code === 0) {
      jwtToken.value = res.data.jwt
      showJwt.value = true
    }
  } catch {
    ElMessage.error('Generate JWT failed')
  }
}

function copyText(text) {
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
</style>

<template>
  <div v-loading="loading">
    <el-page-header @back="$router.push('/dashboard')" :title="'Back'" :content="token?.app_name || 'Token Detail'" />

    <el-row :gutter="20" style="margin-top: 24px" v-if="token">
      <el-col :span="12">
        <el-card>
          <template #header><strong>Key Information</strong></template>
          <el-descriptions :column="1" border>
            <el-descriptions-item label="App ID"><span class="mono">{{ token.app_id }}</span></el-descriptions-item>
            <el-descriptions-item label="App Key">
              <span class="mono">{{ token.app_key }}</span>
              <el-button text size="small" @click="copyText(token.app_key)" style="margin-left: 8px">Copy</el-button>
            </el-descriptions-item>
            <el-descriptions-item label="Status">
              <el-tag :type="token.status === 1 ? 'success' : 'danger'" size="small">{{ token.status === 1 ? 'Active' : 'Disabled' }}</el-tag>
            </el-descriptions-item>
            <el-descriptions-item label="Created">{{ token.created_at }}</el-descriptions-item>
          </el-descriptions>
          <div style="margin-top: 16px; display: flex; gap: 8px">
            <el-popconfirm title="Reset will invalidate existing JWTs. Continue?" @confirm="resetKey">
              <template #reference>
                <el-button type="warning" size="small">Reset Key</el-button>
              </template>
            </el-popconfirm>
            <el-button type="primary" size="small" @click="generateJwt">Generate JWT</el-button>
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

    <el-dialog v-model="showJwt" title="Generated JWT Token" width="600px">
      <el-alert type="warning" :closable="false" style="margin-bottom: 12px">
        This JWT is valid for 30 days. Store it securely.
      </el-alert>
      <el-input type="textarea" :model-value="jwtToken" :rows="6" readonly />
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

async function resetKey() {
  try {
    const res = await api.post(`/api-tokens/${tokenId.value}/reset-key`)
    if (res.code === 0) {
      ElMessage.success('Key reset successfully')
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

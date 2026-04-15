<template>
  <div>
    <div class="page-header">
      <h2>My API Keys</h2>
      <el-button type="primary" @click="showCreate = true">+ New Key</el-button>
    </div>

    <el-table :data="tokens" v-loading="loading" stripe style="width: 100%; margin-top: 16px">
      <el-table-column prop="app_name" label="App Name" min-width="120">
        <template #default="{ row }">{{ row.app_name || '—' }}</template>
      </el-table-column>
      <el-table-column prop="app_id" label="App ID" min-width="180">
        <template #default="{ row }">
          <span class="mono">{{ row.app_id }}</span>
        </template>
      </el-table-column>
      <el-table-column prop="status" label="Status" width="90">
        <template #default="{ row }">
          <el-tag :type="row.status === 1 ? 'success' : 'danger'" size="small">{{ row.status === 1 ? 'Active' : 'Disabled' }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column label="Actions" width="180">
        <template #default="{ row }">
          <el-button text type="primary" size="small" @click="handleGenerateJwt(row)">Get JWT</el-button>
          <el-button text type="primary" size="small" @click="$router.push(`/tokens/${row.id}`)">Detail</el-button>
        </template>
      </el-table-column>
    </el-table>

    <el-dialog v-model="showCreate" title="Create New API Key" width="480px">
      <el-form @submit.prevent="createToken" label-position="top">
        <el-form-item label="App Name">
          <el-input v-model="newForm.appName" placeholder="My Application" />
        </el-form-item>
        <el-form-item label="Refund Address">
          <el-input v-model="newForm.refundAddress" placeholder="0x... (wallet address for refunds)" />
        </el-form-item>
        <el-form-item label="App Fee (%)">
          <el-input-number
            v-model="newForm.appFee"
            :min="0" :max="10" :step="0.5" :precision="2"
            placeholder="0"
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

    <el-dialog v-model="showJwt" title="JWT Token" width="620px" :close-on-click-modal="false">
      <el-alert v-if="isNewToken" type="success" :closable="false" style="margin-bottom: 12px">
        API Key created successfully! Your JWT Token (valid for 30 days):
      </el-alert>
      <el-alert v-else type="warning" :closable="false" style="margin-bottom: 12px">
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
import { ref, onMounted } from 'vue'
import { ElMessage } from 'element-plus'
import api from '../api'

const tokens = ref([])
const loading = ref(false)
const showCreate = ref(false)
const creating = ref(false)
const newForm = ref({ appName: '', refundAddress: '', appFee: 0 })
const showJwt = ref(false)
const jwtToken = ref('')
const isNewToken = ref(false)

async function fetchTokens() {
  loading.value = true
  try {
    const res = await api.get('/api-tokens')
    if (res.code === 0) tokens.value = res.data
  } finally {
    loading.value = false
  }
}

async function createToken() {
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
      ElMessage.success('API Key created successfully')
      showCreate.value = false
      newForm.value = { appName: '', refundAddress: '', appFee: 0 }
      fetchTokens()
      if (res.data.jwt) {
        jwtToken.value = res.data.jwt
        isNewToken.value = true
        showJwt.value = true
      }
    } else {
      ElMessage.error(res.msg)
    }
  } finally {
    creating.value = false
  }
}

async function handleGenerateJwt(row) {
  try {
    const res = await api.post(`/api-tokens/${row.id}/generate-jwt`, { expiresIn: 86400 * 30 })
    if (res.code === 0) {
      jwtToken.value = res.data.jwt
      isNewToken.value = false
      showJwt.value = true
    } else {
      ElMessage.error(res.msg || 'Failed to generate JWT')
    }
  } catch {
    ElMessage.error('Failed to generate JWT')
  }
}

function copyText(text) {
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
</style>

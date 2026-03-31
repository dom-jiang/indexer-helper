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
      <el-table-column prop="app_id" label="App ID" min-width="180" />
      <el-table-column prop="app_key" label="App Key" min-width="200">
        <template #default="{ row }">
          <span class="mono">{{ maskKey(row.app_key) }}</span>
          <el-button text size="small" @click="copyText(row.app_key)">Copy</el-button>
        </template>
      </el-table-column>
      <el-table-column prop="status" label="Status" width="90">
        <template #default="{ row }">
          <el-tag :type="row.status === 1 ? 'success' : 'danger'" size="small">{{ row.status === 1 ? 'Active' : 'Disabled' }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column label="Actions" width="120">
        <template #default="{ row }">
          <el-button text type="primary" size="small" @click="$router.push(`/tokens/${row.id}`)">Detail</el-button>
        </template>
      </el-table-column>
    </el-table>

    <el-dialog v-model="showCreate" title="Create New API Key" width="420px">
      <el-form @submit.prevent="createToken">
        <el-form-item label="App Name">
          <el-input v-model="newAppName" placeholder="My Application" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="showCreate = false">Cancel</el-button>
        <el-button type="primary" :loading="creating" @click="createToken">Create</el-button>
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
const newAppName = ref('')

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
  creating.value = true
  try {
    const res = await api.post('/api-tokens', { appName: newAppName.value })
    if (res.code === 0) {
      ElMessage.success('API Key created successfully')
      showCreate.value = false
      newAppName.value = ''
      fetchTokens()
    } else {
      ElMessage.error(res.msg)
    }
  } finally {
    creating.value = false
  }
}

function maskKey(key) {
  if (!key || key.length < 12) return key
  return key.slice(0, 6) + '****' + key.slice(-4)
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
</style>

<template>
  <div>
    <h2>All API Tokens</h2>
    <el-table :data="tokens" v-loading="loading" stripe style="margin-top: 16px">
      <el-table-column prop="app_id" label="App ID" min-width="160" />
      <el-table-column prop="email" label="Owner" min-width="160" />
      <el-table-column prop="app_name" label="App Name" min-width="120">
        <template #default="{ row }">{{ row.app_name || '—' }}</template>
      </el-table-column>
      <el-table-column prop="status" label="Status" width="90">
        <template #default="{ row }">
          <el-tag :type="row.status === 1 ? 'success' : 'danger'" size="small">{{ row.status === 1 ? 'Active' : 'Disabled' }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column label="Actions" width="220">
        <template #default="{ row }">
          <el-button text size="small" :type="row.status === 1 ? 'danger' : 'success'" @click="toggleStatus(row)">
            {{ row.status === 1 ? 'Disable' : 'Enable' }}
          </el-button>
          <el-button text size="small" type="primary" @click="openRateLimit(row)">Rate Limits</el-button>
          <el-button text size="small" @click="viewUsage(row)">Usage</el-button>
        </template>
      </el-table-column>
    </el-table>
    <el-pagination
      v-if="total > pageSize"
      layout="prev, pager, next"
      :total="total"
      :page-size="pageSize"
      :current-page="page"
      @current-change="p => { page = p; fetchTokens() }"
      style="margin-top: 16px; justify-content: center"
    />

    <!-- Rate Limit Dialog -->
    <el-dialog v-model="showRateLimit" :title="`Rate Limits: ${editAppId}`" width="500px">
      <el-form label-width="140px">
        <el-divider content-position="left">Quote Endpoint</el-divider>
        <el-form-item label="Per Minute">
          <el-input-number v-model="rlForm.quotePerMinute" :min="1" :max="10000" />
        </el-form-item>
        <el-form-item label="Per Month">
          <el-input-number v-model="rlForm.quotePerMonth" :min="1000" :max="100000000" :step="10000" />
        </el-form-item>
        <el-divider content-position="left">Build Endpoint</el-divider>
        <el-form-item label="Per Minute">
          <el-input-number v-model="rlForm.buildPerMinute" :min="1" :max="10000" />
        </el-form-item>
        <el-form-item label="Per Month">
          <el-input-number v-model="rlForm.buildPerMonth" :min="1000" :max="100000000" :step="10000" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="showRateLimit = false">Cancel</el-button>
        <el-button type="primary" :loading="saving" @click="saveRateLimit">Save</el-button>
      </template>
    </el-dialog>

    <!-- Usage Dialog -->
    <el-dialog v-model="showUsage" :title="`Usage: ${usageAppId}`" width="450px">
      <el-descriptions :column="1" border v-if="usageData">
        <el-descriptions-item label="Quote (this minute)">{{ usageData.usage.quote_this_minute }}</el-descriptions-item>
        <el-descriptions-item label="Build (this minute)">{{ usageData.usage.build_this_minute }}</el-descriptions-item>
        <el-descriptions-item label="Total (this month)">{{ usageData.usage.total_this_month }}</el-descriptions-item>
        <el-descriptions-item label="Month">{{ usageData.usage.month }}</el-descriptions-item>
      </el-descriptions>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { ElMessage } from 'element-plus'
import api from '../api'

const tokens = ref([])
const loading = ref(false)
const page = ref(1)
const pageSize = ref(20)
const total = ref(0)

const showRateLimit = ref(false)
const editAppId = ref('')
const saving = ref(false)
const rlForm = ref({ quotePerMinute: 60, quotePerMonth: 300000, buildPerMinute: 30, buildPerMonth: 300000 })

const showUsage = ref(false)
const usageAppId = ref('')
const usageData = ref(null)

async function fetchTokens() {
  loading.value = true
  try {
    const res = await api.get('/admin/tokens', { params: { page: page.value, pageSize: pageSize.value } })
    if (res.code === 0) {
      tokens.value = res.data.list
      total.value = res.data.total
    }
  } finally {
    loading.value = false
  }
}

async function toggleStatus(row) {
  const newStatus = row.status === 1 ? 0 : 1
  await api.put(`/admin/tokens/${row.id}`, { status: newStatus })
  ElMessage.success(newStatus === 1 ? 'Enabled' : 'Disabled')
  fetchTokens()
}

async function openRateLimit(row) {
  editAppId.value = row.app_id
  const res = await api.get(`/admin/tokens/${row.app_id}/rate-limits`)
  if (res.code === 0) {
    const configs = res.data || []
    const quote = configs.find(c => c.endpoint_group === 'quote') || {}
    const build = configs.find(c => c.endpoint_group === 'build') || {}
    rlForm.value = {
      quotePerMinute: quote.per_minute || 60,
      quotePerMonth: quote.per_month || 300000,
      buildPerMinute: build.per_minute || 30,
      buildPerMonth: build.per_month || 300000,
    }
  }
  showRateLimit.value = true
}

async function saveRateLimit() {
  saving.value = true
  try {
    await api.put(`/admin/tokens/${editAppId.value}/rate-limits`, {
      configs: [
        { endpointGroup: 'quote', perMinute: rlForm.value.quotePerMinute, perMonth: rlForm.value.quotePerMonth },
        { endpointGroup: 'build', perMinute: rlForm.value.buildPerMinute, perMonth: rlForm.value.buildPerMonth },
      ],
    })
    ElMessage.success('Rate limits updated')
    showRateLimit.value = false
  } finally {
    saving.value = false
  }
}

async function viewUsage(row) {
  usageAppId.value = row.app_id
  const res = await api.get(`/admin/tokens/${row.app_id}/usage`)
  if (res.code === 0) usageData.value = res.data
  showUsage.value = true
}

onMounted(fetchTokens)
</script>

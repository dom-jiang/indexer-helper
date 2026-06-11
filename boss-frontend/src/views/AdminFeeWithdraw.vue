<template>
  <div>
    <h2>Withdraw Reviews</h2>
    <el-card>
      <div style="display:flex; gap:12px; align-items:center; margin-bottom:12px;">
        <el-select v-model="status" style="width: 200px" @change="fetchRows">
          <el-option label="All" value="" />
          <el-option label="Pending" value="PENDING" />
          <el-option label="Approved" value="APPROVED" />
          <el-option label="Processing" value="PROCESSING" />
          <el-option label="Success" value="SUCCESS" />
          <el-option label="Rejected" value="REJECTED" />
        </el-select>
        <el-button @click="fetchRows">Refresh</el-button>
      </div>
      <el-table :data="rows" v-loading="loading" stripe>
        <el-table-column prop="id" label="ID" width="80" />
        <el-table-column prop="app_id" label="App ID" min-width="160" />
        <el-table-column prop="fee_token_asset" label="Asset" min-width="260" />
        <el-table-column prop="amount" label="Amount" min-width="120" />
        <el-table-column prop="to_chain" label="To Chain" width="110" />
        <el-table-column prop="to_address" label="To Address" min-width="180" />
        <el-table-column prop="status" label="Status" width="120" />
        <el-table-column prop="updated_at" label="Updated At" min-width="160" />
        <el-table-column label="Actions" width="170">
          <template #default="{ row }">
            <el-button
              text
              type="success"
              size="small"
              :disabled="row.status !== 'PENDING'"
              @click="review(row, 'approve')"
            >Approve</el-button>
            <el-button
              text
              type="danger"
              size="small"
              :disabled="row.status !== 'PENDING'"
              @click="review(row, 'reject')"
            >Reject</el-button>
          </template>
        </el-table-column>
      </el-table>
    </el-card>
  </div>
</template>

<script setup>
import { onMounted, ref } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import api from '../api'

const rows = ref([])
const status = ref('')
const loading = ref(false)

async function fetchRows() {
  loading.value = true
  try {
    const res = await api.get('/admin/fee/withdraw-requests', {
      params: { page: 1, pageSize: 200, status: status.value || undefined },
    })
    if (res.code === 0) {
      rows.value = res.data?.list || []
    } else {
      ElMessage.error(res.msg || 'Load failed')
    }
  } finally {
    loading.value = false
  }
}

async function review(row, action) {
  const note = await ElMessageBox.prompt(
    `Input ${action} note (optional)`,
    `${action === 'approve' ? 'Approve' : 'Reject'} Request #${row.id}`,
    { confirmButtonText: 'OK', cancelButtonText: 'Cancel', inputPlaceholder: 'review note' },
  ).then(v => v.value).catch(() => null)
  if (note === null) return
  const res = await api.post(`/admin/fee/withdraw-requests/${row.id}/review`, { action, note })
  if (res.code === 0) {
    ElMessage.success(`Request ${action}d`)
    fetchRows()
  } else {
    ElMessage.error(res.msg || 'Review failed')
  }
}

onMounted(fetchRows)
</script>

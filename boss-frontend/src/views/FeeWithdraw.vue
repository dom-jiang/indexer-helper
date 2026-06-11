<template>
  <div>
    <h2>Fee Withdraw</h2>
    <el-alert
      type="info"
      :closable="false"
      style="margin-bottom: 12px"
      title="Only tokens with estimated value >= 10 USD can be requested."
    />

    <el-card>
      <template #header><strong>Available Balances</strong></template>
      <el-table :data="balances" v-loading="loadingBalances" stripe>
        <el-table-column prop="asset" label="Asset" min-width="280" />
        <el-table-column prop="symbol" label="Symbol" width="100" />
        <el-table-column prop="totalAvailable" label="Available (smallest unit)" min-width="180" />
        <el-table-column prop="availableUsd" label="Est. USD" width="120" />
        <el-table-column label="Action" width="120">
          <template #default="{ row }">
            <el-button
              text
              type="primary"
              :disabled="!row.canWithdraw"
              @click="openApply(row)"
            >Apply</el-button>
          </template>
        </el-table-column>
      </el-table>
    </el-card>

    <el-card style="margin-top: 16px">
      <template #header><strong>Withdraw Requests</strong></template>
      <el-table :data="requests" v-loading="loadingRequests" stripe>
        <el-table-column prop="id" label="ID" width="80" />
        <el-table-column prop="fee_token_asset" label="Asset" min-width="280" />
        <el-table-column prop="amount" label="Amount" min-width="140" />
        <el-table-column prop="to_chain" label="To Chain" width="120" />
        <el-table-column prop="to_address" label="To Address" min-width="200" />
        <el-table-column prop="status" label="Status" width="120" />
        <el-table-column prop="last_error" label="Last Error" min-width="240" />
        <el-table-column prop="updated_at" label="Updated At" min-width="160" />
      </el-table>
    </el-card>

    <el-dialog v-model="showApply" title="Apply Withdraw" width="520px">
      <el-form label-position="top">
        <el-form-item label="Asset">
          <el-input :model-value="form.tokenAsset" disabled />
        </el-form-item>
        <el-form-item label="Amount (smallest unit)">
          <el-input v-model="form.amount" />
        </el-form-item>
        <el-form-item label="To Chain">
          <el-select v-model="form.toChain" style="width: 100%" filterable>
            <el-option v-for="c in chains" :key="c" :label="c" :value="c" />
          </el-select>
        </el-form-item>
        <el-form-item label="To Address">
          <el-input v-model="form.toAddress" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="showApply = false">Cancel</el-button>
        <el-button type="primary" :loading="submitting" @click="submitApply">Submit</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { onMounted, ref } from 'vue'
import { ElMessage } from 'element-plus'
import api from '../api'

const loadingBalances = ref(false)
const loadingRequests = ref(false)
const submitting = ref(false)
const balances = ref([])
const requests = ref([])
const chains = ref([])
const showApply = ref(false)
const form = ref({
  tokenAsset: '',
  amount: '',
  toChain: '',
  toAddress: '',
})

async function fetchBalances() {
  loadingBalances.value = true
  try {
    const res = await api.get('/fee/balances')
    if (res.code === 0) {
      balances.value = res.data?.tokens || []
    } else {
      ElMessage.error(res.msg || 'Failed to load balances')
    }
  } finally {
    loadingBalances.value = false
  }
}

async function fetchOptions() {
  const res = await api.get('/fee/withdraw-options')
  if (res.code === 0) {
    chains.value = res.data?.chains || []
  }
}

async function fetchRequests() {
  loadingRequests.value = true
  try {
    const res = await api.get('/fee/withdraw-requests', { params: { page: 1, pageSize: 100 } })
    if (res.code === 0) {
      requests.value = res.data?.list || []
    } else {
      ElMessage.error(res.msg || 'Failed to load requests')
    }
  } finally {
    loadingRequests.value = false
  }
}

function openApply(row) {
  form.value = {
    tokenAsset: row.asset,
    amount: row.totalAvailable,
    toChain: chains.value[0] || '',
    toAddress: '',
  }
  showApply.value = true
}

async function submitApply() {
  if (!form.value.tokenAsset || !form.value.amount || !form.value.toChain || !form.value.toAddress) {
    ElMessage.warning('Please fill all fields')
    return
  }
  submitting.value = true
  try {
    const res = await api.post('/fee/withdraw-requests', { ...form.value })
    if (res.code === 0) {
      ElMessage.success('Withdraw request submitted')
      showApply.value = false
      await fetchBalances()
      await fetchRequests()
    } else {
      ElMessage.error(res.msg || 'Submit failed')
    }
  } finally {
    submitting.value = false
  }
}

onMounted(async () => {
  await fetchOptions()
  await fetchBalances()
  await fetchRequests()
})
</script>

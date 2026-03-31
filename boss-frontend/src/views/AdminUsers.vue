<template>
  <div>
    <h2>User Management</h2>
    <el-table :data="users" v-loading="loading" stripe style="margin-top: 16px">
      <el-table-column prop="id" label="ID" width="60" />
      <el-table-column prop="email" label="Email" min-width="200" />
      <el-table-column prop="role" label="Role" width="100">
        <template #default="{ row }">
          <el-tag :type="row.role === 'admin' ? 'danger' : 'info'" size="small">{{ row.role }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column prop="status" label="Status" width="90">
        <template #default="{ row }">
          <el-tag :type="row.status === 1 ? 'success' : 'danger'" size="small">{{ row.status === 1 ? 'Active' : 'Disabled' }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column prop="created_at" label="Created" min-width="160" />
      <el-table-column label="Actions" width="200">
        <template #default="{ row }">
          <el-button text size="small" @click="toggleRole(row)">
            {{ row.role === 'admin' ? 'Set User' : 'Set Admin' }}
          </el-button>
          <el-button text size="small" :type="row.status === 1 ? 'danger' : 'success'" @click="toggleStatus(row)">
            {{ row.status === 1 ? 'Disable' : 'Enable' }}
          </el-button>
        </template>
      </el-table-column>
    </el-table>
    <el-pagination
      v-if="total > pageSize"
      layout="prev, pager, next"
      :total="total"
      :page-size="pageSize"
      :current-page="page"
      @current-change="p => { page = p; fetchUsers() }"
      style="margin-top: 16px; justify-content: center"
    />
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { ElMessage } from 'element-plus'
import api from '../api'

const users = ref([])
const loading = ref(false)
const page = ref(1)
const pageSize = ref(20)
const total = ref(0)

async function fetchUsers() {
  loading.value = true
  try {
    const res = await api.get('/admin/users', { params: { page: page.value, pageSize: pageSize.value } })
    if (res.code === 0) {
      users.value = res.data.list
      total.value = res.data.total
    }
  } finally {
    loading.value = false
  }
}

async function toggleRole(row) {
  const newRole = row.role === 'admin' ? 'user' : 'admin'
  await api.put(`/admin/users/${row.id}`, { role: newRole })
  ElMessage.success(`Role changed to ${newRole}`)
  fetchUsers()
}

async function toggleStatus(row) {
  const newStatus = row.status === 1 ? 0 : 1
  await api.put(`/admin/users/${row.id}`, { status: newStatus })
  ElMessage.success(newStatus === 1 ? 'Enabled' : 'Disabled')
  fetchUsers()
}

onMounted(fetchUsers)
</script>

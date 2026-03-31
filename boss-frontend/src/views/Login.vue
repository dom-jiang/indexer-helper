<template>
  <div class="login-page">
    <div class="login-card">
      <h2 class="login-title">API Management</h2>
      <p class="login-subtitle">{{ isRegister ? 'Create your account' : 'Sign in to your account' }}</p>

      <el-form :model="form" @submit.prevent="handleSubmit" class="login-form">
        <el-form-item>
          <el-input v-model="form.email" placeholder="Email" size="large" prefix-icon="Message" />
        </el-form-item>
        <el-form-item>
          <el-input v-model="form.password" placeholder="Password" type="password" size="large" prefix-icon="Lock" show-password />
        </el-form-item>
        <el-form-item>
          <el-button type="primary" size="large" :loading="loading" @click="handleSubmit" style="width: 100%">
            {{ isRegister ? 'Register' : 'Login' }}
          </el-button>
        </el-form-item>
      </el-form>

      <div class="login-switch">
        <span v-if="!isRegister">Don't have an account? <el-link type="primary" @click="isRegister = true">Register</el-link></span>
        <span v-else>Already have an account? <el-link type="primary" @click="isRegister = false">Login</el-link></span>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { useAuthStore } from '../store'
import api from '../api'

const router = useRouter()
const auth = useAuthStore()

const isRegister = ref(false)
const loading = ref(false)
const form = ref({ email: '', password: '' })

async function handleSubmit() {
  if (!form.value.email || !form.value.password) {
    ElMessage.warning('Please fill in all fields')
    return
  }
  loading.value = true
  try {
    const endpoint = isRegister.value ? '/register' : '/login'
    const res = await api.post(endpoint, form.value)
    if (res.code === 0) {
      auth.setAuth(res.data.user, res.data.token)
      ElMessage.success(isRegister.value ? 'Registration successful' : 'Login successful')
      router.push('/dashboard')
    } else {
      ElMessage.error(res.msg || 'Operation failed')
    }
  } catch (e) {
    ElMessage.error(e.response?.data?.msg || 'Network error')
  } finally {
    loading.value = false
  }
}
</script>

<style scoped>
.login-page {
  min-height: 100vh;
  display: flex;
  align-items: center;
  justify-content: center;
  background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
}
.login-card {
  background: white;
  border-radius: 12px;
  padding: 40px;
  width: 400px;
  box-shadow: 0 20px 60px rgba(0,0,0,0.2);
}
.login-title {
  text-align: center;
  font-size: 24px;
  font-weight: 700;
  color: #303133;
  margin-bottom: 4px;
}
.login-subtitle {
  text-align: center;
  color: #909399;
  margin-bottom: 28px;
  font-size: 14px;
}
.login-form {
  margin-top: 20px;
}
.login-switch {
  text-align: center;
  font-size: 13px;
  color: #909399;
}
</style>

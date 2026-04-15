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
        <el-form-item v-if="isRegister">
          <div class="code-row">
            <el-input v-model="form.code" placeholder="Verification Code" size="large" maxlength="6" />
            <el-button
              size="large"
              :disabled="countdown > 0 || !form.email"
              :loading="sendingCode"
              @click="handleSendCode"
              class="code-btn"
            >
              {{ countdown > 0 ? `${countdown}s` : 'Send Code' }}
            </el-button>
          </div>
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
import { ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { useAuthStore } from '../store'
import api from '../api'

const router = useRouter()
const auth = useAuthStore()

const isRegister = ref(false)
const loading = ref(false)
const sendingCode = ref(false)
const countdown = ref(0)
const form = ref({ email: '', password: '', code: '' })

let countdownTimer = null

watch(isRegister, () => {
  form.value.code = ''
})

function startCountdown() {
  countdown.value = 60
  countdownTimer = setInterval(() => {
    countdown.value--
    if (countdown.value <= 0) {
      clearInterval(countdownTimer)
      countdownTimer = null
    }
  }, 1000)
}

async function handleSendCode() {
  if (!form.value.email || !form.value.email.includes('@')) {
    ElMessage.warning('Please enter a valid email first')
    return
  }
  sendingCode.value = true
  try {
    const res = await api.post('/send-code', { email: form.value.email })
    if (res.code === 0) {
      ElMessage.success('Verification code sent to your email')
      startCountdown()
    } else {
      ElMessage.error(res.msg || 'Failed to send code')
    }
  } catch (e) {
    ElMessage.error(e.response?.data?.msg || 'Network error')
  } finally {
    sendingCode.value = false
  }
}

async function handleSubmit() {
  if (!form.value.email || !form.value.password) {
    ElMessage.warning('Please fill in all fields')
    return
  }
  if (isRegister.value && !form.value.code) {
    ElMessage.warning('Please enter the verification code')
    return
  }
  loading.value = true
  try {
    const endpoint = isRegister.value ? '/register' : '/login'
    const payload = isRegister.value
      ? { email: form.value.email, password: form.value.password, code: form.value.code }
      : { email: form.value.email, password: form.value.password }
    const res = await api.post(endpoint, payload)
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
  width: 420px;
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
.code-row {
  display: flex;
  gap: 10px;
  width: 100%;
}
.code-row .el-input {
  flex: 1;
}
.code-btn {
  min-width: 110px;
}
</style>

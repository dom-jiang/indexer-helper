<template>
  <el-container class="layout">
    <el-aside width="220px" class="sidebar">
      <div class="logo">API Boss</div>
      <el-menu :default-active="activeMenu" router class="sidebar-menu">
        <el-menu-item index="/dashboard">
          <el-icon><HomeFilled /></el-icon>
          <span>Dashboard</span>
        </el-menu-item>
        <template v-if="auth.isAdmin">
          <el-menu-item-group title="Admin">
            <el-menu-item index="/admin/users">
              <el-icon><User /></el-icon>
              <span>Users</span>
            </el-menu-item>
            <el-menu-item index="/admin/tokens">
              <el-icon><Key /></el-icon>
              <span>All Tokens</span>
            </el-menu-item>
          </el-menu-item-group>
        </template>
      </el-menu>
    </el-aside>
    <el-container>
      <el-header class="header">
        <span class="header-email">{{ auth.user?.email }}</span>
        <el-tag v-if="auth.isAdmin" type="danger" size="small" style="margin-left: 8px">Admin</el-tag>
        <el-button text type="primary" @click="handleLogout" style="margin-left: 16px">Logout</el-button>
      </el-header>
      <el-main class="main-content">
        <router-view />
      </el-main>
    </el-container>
  </el-container>
</template>

<script setup>
import { computed } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { HomeFilled, User, Key } from '@element-plus/icons-vue'
import { useAuthStore } from '../store'

const route = useRoute()
const router = useRouter()
const auth = useAuthStore()

const activeMenu = computed(() => route.path)

function handleLogout() {
  auth.logout()
  router.push('/login')
}
</script>

<style scoped>
.layout {
  min-height: 100vh;
}
.sidebar {
  background: #1d1e2c;
  overflow-y: auto;
}
.logo {
  height: 60px;
  display: flex;
  align-items: center;
  justify-content: center;
  color: white;
  font-size: 20px;
  font-weight: 700;
  letter-spacing: 1px;
  border-bottom: 1px solid rgba(255,255,255,0.08);
}
.sidebar-menu {
  border-right: none;
  background: transparent;
  --el-menu-bg-color: transparent;
  --el-menu-text-color: rgba(255,255,255,0.7);
  --el-menu-hover-bg-color: rgba(255,255,255,0.08);
  --el-menu-active-color: #409eff;
}
.header {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  background: white;
  border-bottom: 1px solid #ebeef5;
  padding: 0 24px;
  height: 60px;
}
.header-email {
  font-size: 14px;
  color: #606266;
}
.main-content {
  background: #f5f7fa;
  padding: 24px;
}
</style>

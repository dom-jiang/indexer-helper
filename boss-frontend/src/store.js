import { defineStore } from 'pinia'
import { ref, computed } from 'vue'

export const useAuthStore = defineStore('auth', () => {
  const token = ref(localStorage.getItem('boss_token') || '')
  const user = ref(JSON.parse(localStorage.getItem('boss_user') || 'null'))

  const isLoggedIn = computed(() => !!token.value)
  const isAdmin = computed(() => user.value?.role === 'admin')

  function setAuth(userData, tokenStr) {
    user.value = userData
    token.value = tokenStr
    localStorage.setItem('boss_token', tokenStr)
    localStorage.setItem('boss_user', JSON.stringify(userData))
  }

  function logout() {
    user.value = null
    token.value = ''
    localStorage.removeItem('boss_token')
    localStorage.removeItem('boss_user')
  }

  return { token, user, isLoggedIn, isAdmin, setAuth, logout }
})

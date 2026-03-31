import axios from 'axios'

const api = axios.create({
  baseURL: '/boss',
  timeout: 15000,
})

api.interceptors.request.use(config => {
  const token = localStorage.getItem('boss_token')
  if (token) {
    config.headers.Authorization = `Bearer ${token}`
  }
  return config
})

api.interceptors.response.use(
  res => res.data,
  err => {
    if (err.response?.status === 401) {
      localStorage.removeItem('boss_token')
      localStorage.removeItem('boss_user')
      window.location.hash = '#/login'
    }
    return Promise.reject(err)
  }
)

export default api

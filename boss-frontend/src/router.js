import { createRouter, createWebHashHistory } from 'vue-router'

const routes = [
  {
    path: '/login',
    name: 'Login',
    component: () => import('./views/Login.vue'),
  },
  {
    path: '/',
    component: () => import('./views/Layout.vue'),
    redirect: '/dashboard',
    children: [
      { path: 'dashboard', name: 'Dashboard', component: () => import('./views/Dashboard.vue') },
      { path: 'tokens/:id', name: 'TokenDetail', component: () => import('./views/TokenDetail.vue') },
      { path: 'guide', name: 'Guide', component: () => import('./views/Guide.vue') },
      { path: 'admin/users', name: 'AdminUsers', component: () => import('./views/AdminUsers.vue'), meta: { admin: true } },
      { path: 'admin/tokens', name: 'AdminTokens', component: () => import('./views/AdminTokens.vue'), meta: { admin: true } },
    ],
  },
]

const router = createRouter({
  history: createWebHashHistory(),
  routes,
})

router.beforeEach((to, from, next) => {
  const token = localStorage.getItem('boss_token')
  if (to.name !== 'Login' && !token) {
    next({ name: 'Login' })
  } else {
    next()
  }
})

export default router

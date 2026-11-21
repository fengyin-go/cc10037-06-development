import { createRouter, createWebHistory } from 'vue-router'

import BackfillResumeView from '@/views/BackfillResumeView.vue'
import Overview from '@/views/Overview.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    { path: '/backfill-resume', name: 'backfill-resume', component: BackfillResumeView },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router

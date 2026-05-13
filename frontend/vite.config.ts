import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

const ingestionProxyTarget = process.env['FRONTEND_INGESTION_PROXY_TARGET'] ?? 'http://127.0.0.1:9051'
const realtimeProxyTarget = process.env['FRONTEND_REALTIME_PROXY_TARGET'] ?? 'ws://127.0.0.1:9052'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    port: 9059,
    open: false,
    allowedHosts: ['localhost', '127.0.0.1', 'clickstream-frontend'],
    proxy: {
      '/api': {
        target: ingestionProxyTarget,
        changeOrigin: true,
        configure: (proxy) => {
          proxy.on('proxyReq', (proxyReq) => {
            proxyReq.removeHeader('origin')
          })
        },
      },
      '/ws': {
        target: realtimeProxyTarget,
        ws: true,
        configure: (proxy) => {
          proxy.on('proxyReqWs', (proxyReq) => {
            proxyReq.removeHeader('origin')
          })
        },
      },
    },
  },
})

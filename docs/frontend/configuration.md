# Frontend Configuration Guide

Complete environment setup and configuration reference for the React frontend.

## Environment Variables

### Development (Vite Dev Server)

| Variable | Default Value | Purpose | Required |
|----------|----------------|---------|----------|
| `VITE_API_BASE_URL` | `http://localhost:8081` | Backend Ingestion API endpoint | No |
| `VITE_WS_URL` | `ws://localhost:8082` | Real-time Analytics WebSocket | No |

### Creating .env File

Create `frontend/.env` in the project root:

```env
# Backend API
VITE_API_BASE_URL=http://localhost:8081

# Real-time WebSocket
VITE_WS_URL=ws://localhost:8082
```

**Note:** All `VITE_*` variables are automatically exposed to the browser code. Variables NOT prefixed with `VITE_` are private to the build process.

### Production Build

During `npm run build`, Vite replaces all `import.meta.env.VITE_*` references with actual values:

```typescript
// In your code
const API_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8081'
const WS_URL = import.meta.env.VITE_WS_URL || 'ws://localhost:8082'

// After build:
const API_URL = 'http://localhost:8081'  // Replaced at build time
const WS_URL = 'ws://localhost:8082'
```

---

## Vite Configuration

### Development Server Config

File: `frontend/vite.config.ts`

```typescript
export default defineConfig({
  plugins: [react()],
  server: {
    port: 3000,
    proxy: {
      // Proxy /api/* requests to Ingestion API
      '/api': {
        target: 'http://localhost:8081',
        changeOrigin: true,
      },
      // Proxy /ws/* WebSocket connections
      '/ws': {
        target: 'ws://localhost:8082',
        ws: true,
      },
    },
  },
})
```

### Proxy Behavior

During development (`npm run dev`), Vite proxies requests:

```
Browser Request     Dev Server      Backend
────────────────    ──────────────  ────────────
GET /api/... ────→ /api proxy   ──→ :8081/api/...
POST /api/... ───→ /api proxy   ──→ :8081/api/...
ws://... ─────────→ /ws proxy   ──→ ws://localhost:8082
```

**Benefits:**
- No CORS headers needed during development
- Same origin for all requests
- Simplifies API calls (`GET /api/sessions` instead of `GET http://localhost:8081/api/sessions`)

### API URL in Production

In production (after `npm run build`), update the API endpoint in your deployment environment:

```env
# frontend/.env.production
VITE_API_BASE_URL=https://api.example.com
VITE_WS_URL=wss://api.example.com
```

**Protocol considerations:**
- Development: `http://` and `ws://` (insecure, OK for localhost)
- Production: `https://` and `wss://` (HTTPS required)

---

## TypeScript Configuration

File: `frontend/tsconfig.json`

```json
{
  "compilerOptions": {
    "target": "ES2020",              // Modern JavaScript
    "module": "ESNext",              // ES modules
    "jsx": "react-jsx",              // React 19 JSX
    "strict": true,                  // Full type checking
    "moduleResolution": "bundler",   // Vite module resolution
    "skipLibCheck": true,            // Skip type checking of dependencies
    "noUnusedLocals": true,          // Error on unused variables
    "noUnusedParameters": true,      // Error on unused parameters
    "noFallthroughCasesInSwitch": true  // Error on missing break in switch
  },
  "include": ["src"]
}
```

### Recommended ESLint Config

File: `frontend/.eslintrc.js`

```javascript
export default [
  {
    files: ['**/*.{js,jsx,ts,tsx}'],
    languageOptions: {
      parser: '@typescript-eslint/parser',
      parserOptions: {
        ecmaVersion: 2020,
        sourceType: 'module',
        ecmaFeatures: { jsx: true },
      },
      globals: {
        React: 'readonly',
      },
    },
    rules: {
      'react/react-in-jsx-scope': 'off',  // Not needed in React 19
      'no-unused-vars': 'off',
      '@typescript-eslint/no-unused-vars': ['error'],
    },
  },
]
```

---

## API Endpoints

### Ingestion API (http://localhost:8081)

| Method | Endpoint | Purpose | Request Body |
|--------|----------|---------|--------------|
| POST | `/api/events` | Single event ingestion | ClickEvent JSON |
| POST | `/api/events/batch` | Batch event ingestion (recommended) | `{ "events": [...] }` |
| GET | `/api/analytics/sessions` | Fetch session history | Query params: `?page=0&size=10` |
| GET | `/api/analytics/pages` | Fetch page metrics | Query params: `?page=0&size=10` |
| GET | `/api/analytics/journeys` | Fetch user journeys | Query params: `?sessionId=...` |

### Real-time Analytics WebSocket (ws://localhost:8082)

**Protocol:** Arrow IPC over WebSocket

**Connection Flow:**
1. Browser connects to `ws://localhost:8082`
2. Server responds with first Arrow TableRecordBatch
3. Subsequent batches stream in real-time
4. Connection auto-reconnects on close

**Data Format:**
```javascript
// Received from server: Arrow IPC binary data
// Parsed by: useRealtimeMetrics hook
// Converted to: JavaScript objects (metrics array)

[
  { name: 'total_sessions', value: 1024 },
  { name: 'total_clicks', value: 52341 },
  { name: 'avg_session_duration', value: 342 },
  // ... more metrics
]
```

---

## Dependencies

### Production Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| `react` | ^19.0.0 | UI framework |
| `react-dom` | ^19.0.0 | React rendering |
| `react-router-dom` | ^7.1.3 | Routing and navigation |
| `@tanstack/react-query` | ^5.62.11 | Data fetching and caching |
| `apache-arrow` | ^17.0.0 | Arrow IPC parsing for real-time data |
| `recharts` | ^2.15.0 | Charting library for sparklines |
| `axios` | ^1.7.9 | HTTP client (for REST API calls) |

### Development Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| `typescript` | ~5.6.2 | TypeScript compiler |
| `vite` | ^6.0.11 | Build tool and dev server |
| `vitest` | ^2.1.8 | Unit testing framework |
| `@testing-library/react` | ^16.1.0 | React component testing |
| `@vitejs/plugin-react` | ^4.3.4 | Vite React plugin |
| `eslint` | ^9.17.0 | Code linting |

---

## Build Process

### Development Build

```bash
npm run dev
```

- Vite dev server starts on port 3000
- Hot module replacement (HMR) enabled
- Source maps available for debugging
- No optimizations (fast reload)

### Production Build

```bash
npm run build
```

**Process:**
1. TypeScript compilation: `tsc -b` (check for type errors)
2. Vite bundling: `vite build` (create optimized dist/)
3. Code splitting applied automatically
4. Assets minified and fingerprinted

**Output:**
```
dist/
├── index.html          # Entry point
├── assets/
│   ├── index-XXXXX.js  # Main bundle (minified)
│   ├── vendor-XXXXX.js # React + dependencies
│   └── style-XXXXX.css # Global styles
└── ...
```

**Optimization:**
- Tree-shaking removes unused code
- Chunk splitting separates vendor code
- Asset compression (gzip)
- CSS extraction to separate files

### Preview Production Build

```bash
npm run preview
```

- Builds the project
- Serves dist/ directory locally
- Useful for testing production minified code

---

## Testing Configuration

### Vitest Config

File: `frontend/vitest.config.ts`

```typescript
import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    environment: 'jsdom',
    globals: true,
    setupFiles: ['./src/test/setup.ts'],
  },
})
```

### Writing Tests

```typescript
import { render, screen, fireEvent } from '@testing-library/react'
import { MetricValue } from './MetricValue'

describe('MetricValue', () => {
  it('renders label, value, and unit', () => {
    render(<MetricValue label="Sessions" value={42} unit="total" />)
    
    expect(screen.getByText('Sessions')).toBeInTheDocument()
    expect(screen.getByText('42')).toBeInTheDocument()
    expect(screen.getByText('total')).toBeInTheDocument()
  })

  it('render value without unit if not provided', () => {
    render(<MetricValue label="Metric" value={100} />)
    
    expect(screen.getByText('100')).toBeInTheDocument()
    // Should not have a unit span
    expect(screen.queryByClass('metric-unit')).not.toBeInTheDocument()
  })
})
```

---

## Deploying to Production

### Static Hosting (Vercel, Netlify, GitHub Pages)

1. **Build the project:**
   ```bash
   npm run build
   ```

2. **Set environment variables** in your hosting dashboard:
   ```
   VITE_API_BASE_URL = https://api.example.com
   VITE_WS_URL = wss://api.example.com
   ```

3. **Deploy dist/ directory**

### Docker Deployment

```dockerfile
# frontend/Dockerfile
FROM node:18-alpine as builder
WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . .
RUN npm run build

FROM nginx:alpine
COPY --from=builder /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
```

### Nginx Configuration

```nginx
# frontend/nginx.conf
server {
    listen 80;
    server_name _;

    root /usr/share/nginx/html;
    index index.html;

    # SPA routing: serve index.html for non-existent files
    try_files $uri $uri/ /index.html;

    # API proxy
    location /api/ {
        proxy_pass http://api:8081;
    }

    # WebSocket proxy
    location /ws/ {
        proxy_pass http://realtime:8082;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
    }
}
```

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Port 3000 already in use | Change `server.port` in vite.config.ts |
| CORS errors | Verify Vite proxy config and backend CORS settings |
| Environment variables undefined | Ensure variables prefixed with `VITE_`, restart dev server |
| WebSocket connection fails | Check realtime-analytics running on port 8082 |
| Build fails with TypeScript errors | Run `npm run lint` and fix issues |
| Dependencies not installing | Delete node_modules, run `npm install` again |

---

## Related Documentation

- [Component Architecture](./components.md)
- [Event Tracking Guide](./event-tracking.md)
- [Integration Testing](./integration-testing.md)

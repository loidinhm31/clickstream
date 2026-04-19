# Frontend Module - React + TypeScript

**Clickstream Analytics Dashboard** — Real-time and historical analytics frontend built with React 19, TypeScript, and Atomic Design architecture. Displays live metrics via WebSocket, historical session data, and user journey analysis.

**Phase:** 07 - React Frontend  
**Status:** ✅ Complete  
**Files Changed:** 60+  
**Technology Stack:** React 19, TypeScript, Vite, TanStack Query, Apache Arrow, Recharts

---

## 🏗️ Architecture

### Component Hierarchy (Atomic Design)

```
Atoms (Pure UI Primitives)
├── Button - Interactive elements
├── Badge - Status indicators (status-dot)
├── MetricValue - Single metric display (label + number + unit)
├── Spinner - Loading states
├── Sparkline - Mini charts
└── StatusDot - Connection/status indicators

Molecules (Composed Components)
├── MetricCard - Metric + sparkline (status-dot integrated)
├── SessionRow - Table row for session display
└── PageViewRow - Table row for page metrics

Organisms (Business Logic + Tracking)
├── RealtimeDashboard - Grid of metric cards with WebSocket updates
├── SessionTable - Full session history with pagination
├── PageMetrics - Page analytics table
└── NavigationBar - App navigation with event tracking

Templates
└── DashboardTemplate - Layout wrapper (sidebar + main)

Pages (Route Components + PAGE_VIEW tracking)
├── DashboardPage - Real-time metrics dashboard
├── SessionsPage - Historical sessions table
├── JourneysPage - User journey analysis
└── PagesAnalyticsPage - Page-level analytics
```

### Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────┐
│ React App (TrackingContext + RealtimeContext)                │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  Real-time Path:                                              │
│  ┌──────────────────────────────────────────────────────┐   │
│  │ WebSocket (ws://localhost:9052)                      │   │
│  │ ↓ Arrow IPC (Arrow TableRecordBatches)               │   │
│  │ RealtimeContext (manages connection + reconnection) │   │
│  │ ↓ useRealtimeMetrics hook                            │   │
│  │ RealtimeDashboard (renders metrics cards)           │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                               │
│  Historical Path:                                             │
│  ┌──────────────────────────────────────────────────────┐   │
│  │ REST API (http://localhost:9051/api/analytics/*)   │   │
│  │ ↓ TanStack Query (data fetching + caching)           │   │
│  │ SessionTable / PageMetrics (render from JSON)       │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                               │
│  Event Tracking Path:                                         │
│  ┌──────────────────────────────────────────────────────┐   │
│  │ TrackingContext (sessionId + userId)                │   │
│  │ ↓ User interaction (click, page view)                │   │
│  │ trackEvent() → Event queue                           │   │
│  │ ↓ Batching (10 events OR 2 seconds timeout)          │   │
│  │ POST /api/events/batch → Ingestion API              │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

---

## 🚀 Quick Start

### Prerequisites

- Node.js 18+ and npm
- Backend services running:
  - Kafka (9056)
  - MongoDB (9055)
  - Ingestion API (9051)
  - Real-time Analytics (9052)

### Installation & Development

```bash
# Navigate to frontend directory
cd frontend

# Install dependencies
npm install

# Start dev server (http://localhost:3000)
npm run dev

# In another terminal, run tests
npm test

# Build for production
npm run build

# Run ESLint
npm run lint
```

**Development server output:**
```
VITE v6.0.11  ready in 245 ms

➜  Local:   http://localhost:3000/
➜  Press h to show help
```

---

## ⚙️ Environment Configuration

### Environment Variables

| Variable | Value | Purpose | Required |
|----------|-------|---------|----------|
| `VITE_API_BASE_URL` | `http://localhost:9051` | Ingestion API base URL | No (defaults to current origin) |
| `VITE_WS_URL` | `ws://localhost:9052` | WebSocket endpoint for real-time metrics | No (defaults to ws://localhost:9052) |

**Development:** Vite dev server proxies all `/api/*` and `/ws/*` requests automatically (see vite.config.ts)

### .env File (Optional)

Create `frontend/.env` for custom configuration:

```env
VITE_API_BASE_URL=http://backend.example.com:9051
VITE_WS_URL=ws://backend.example.com:9052
```

**Note:** Environment variables must be prefixed with `VITE_` to be exposed to the browser.

---

## 📊 Key Features

### 1. Real-time Metrics Dashboard

**WebSocket Connection** → Arrow IPC Protocol → Live metric cards

- Auto-reconnection with exponential backoff (1s → 30s max)
- Automatic retry on connection loss
- Status indicator (green = connected, red = disconnected)
- Auto-refresh on activity (30-minute session timeout)

**Metrics displayed:**
- Total unique sessions (last 24h)
- Total clicks (last 24h)
- Average session duration
- Top pages (by views)
- Bounce rate

### 2. Event Tracking System

**Automatic event batching** reduces network overhead:

- **Batch trigger:** 10 events OR 2 seconds timeout (whichever comes first)
- **Event types:** CLICK, PAGE_VIEW, SCROLL, HOVER
- **Session management:** 30-minute timeout with auto-renewal on activity
- **Storage:** Session ID stored in sessionStorage, persistent across page refreshes

**Tracked interactions:**
- Page views (automatic on route change)
- Button clicks (Organism level)
- Navigation clicks
- Table interactions
- Form submissions

### 3. Session Management (30min Timeout)

```javascript
// Session creation (automatic)
sessionId = UUID v4 (crypto.randomUUID())
sessionStorage.setItem('clickstream_session_id', sessionId)
sessionStorage.setItem('clickstream_session_timestamp', now)

// Session validation
if (now - storedTimestamp < 30 * 60 * 1000) {
  // Session valid, update timestamp
  sessionStorage.setItem('clickstream_session_timestamp', now)
  return sessionId
} else {
  // Session expired, create new
}

// Auto-renewal on any activity
updateTimestamp() on click, page view, scroll, etc.
```

### 4. Historical Analytics

**Data querying via TanStack Query:**

- Automatic caching (5-minute stale time)
- Background refetching
- Request deduplication
- Error boundaries with retry

**Available endpoints:**
- `GET /api/analytics/sessions` — Session history with pagination
- `GET /api/analytics/pages` — Page metrics
- `GET /api/analytics/journeys` — User journey sequences

---

## 📁 Directory Structure

```
frontend/
├── src/
│   ├── main.tsx                 # Entry point
│   ├── App.tsx                  # Router setup
│   ├── components/
│   │   ├── atoms/               # UI primitives
│   │   │   ├── Button.tsx
│   │   │   ├── Badge.tsx
│   │   │   ├── MetricValue.tsx
│   │   │   ├── Spinner.tsx
│   │   │   ├── Sparkline.tsx
│   │   │   ├── StatusDot.tsx
│   │   │   └── __tests__/       # Unit tests
│   │   ├── molecules/           # Composite components
│   │   │   ├── MetricCard.tsx
│   │   │   ├── SessionRow.tsx
│   │   │   └── PageViewRow.tsx
│   │   ├── organisms/           # Business logic + tracking
│   │   │   ├── RealtimeDashboard.tsx
│   │   │   ├── SessionTable.tsx
│   │   │   ├── PageMetrics.tsx
│   │   │   └── NavigationBar.tsx
│   │   ├── templates/           # Layouts
│   │   │   └── DashboardTemplate.tsx
│   │   ├── pages/               # Route components
│   │   │   ├── DashboardPage.tsx
│   │   │   ├── SessionsPage.tsx
│   │   │   ├── JourneysPage.tsx
│   │   │   └── PagesAnalyticsPage.tsx
│   │   ├── ErrorBoundary.tsx
│   │   └── ...
│   ├── contexts/                # Context APIs
│   │   ├── TrackingContext.tsx  # Session + event tracking
│   │   ├── RealtimeContext.tsx  # WebSocket + Arrow metrics
│   │   └── ...
│   ├── hooks/                   # Custom React hooks
│   │   ├── useRealtimeMetrics.ts
│   │   ├── useTrackEvent.ts
│   │   ├── useBatchedEvents.ts
│   │   └── ...
│   ├── styles/                  # Global CSS
│   └── types/                   # TypeScript types
├── public/                      # Static assets
├── vite.config.ts              # Vite configuration (proxy setup)
├── vitest.config.ts            # Vitest configuration
├── tsconfig.json               # TypeScript configuration
├── package.json                # Dependencies
└── README.md                   # This file
```

---

## 🧪 Testing Strategy

### Test Levels

| Level | Tool | Coverage | Examples |
|-------|------|----------|----------|
| **Unit** | Vitest + React Testing Library | Component logic | MetricValue.test.tsx, Button.test.tsx |
| **Integration** | Vitest + MSW (mock backend) | Component + hooks | RealtimeDashboard integration test |
| **E2E** | Cypress (optional) | Full user workflows | Login → dashboard → event submission |

### Running Tests

```bash
# Run all tests (watch mode)
npm test

# Run single test file
npm test MetricValue.test.tsx

# Run with coverage
npm test -- --coverage

# Run specific test type
npm test -- --grep "MetricValue"
```

### Testing Checklist

- [ ] Atom components render without errors
- [ ] Molecule components compose atoms correctly
- [ ] Organisms track events properly
- [ ] WebSocket connection establishes and reconnects
- [ ] Event batching triggers correctly (10 events OR 2s)
- [ ] Session timeout extends on activity
- [ ] TanStack Query caches data correctly
- [ ] Error boundaries catch and display errors

---

## 🔌 Integration Testing Guide

### Prerequisites

Before running integration tests, ensure these services are running:

```bash
# Terminal 1: Infrastructure
docker compose up -d

# Terminal 2: Ingestion API (Spring Boot)
cd ../ingestion-api
mvn spring-boot:run  # Port 9051

# Terminal 3: Real-time Analytics (Arrow + WebSocket)
cd ../realtime-analytics
python -m realtime_analytics.server  # Port 9052

# Terminal 4: Frontend
cd frontend
npm run dev  # Port 3000
```

### Integration Test Scenarios

#### 1. Event Submission Flow

```typescript
// Test: User interaction → Event batching → Ingestion API → Kafka
test('Batch 10 clicks and verify Kafka message', async () => {
  const { rerender } = render(<App />)
  
  // Simulate 10 clicks
  for (let i = 0; i < 10; i++) {
    fireEvent.click(screen.getByRole('button'))
  }
  
  // Verify batch POST to /api/events/batch
  await waitFor(() => {
    expect(mockFetch).toHaveBeenCalledWith(
      'http://localhost:9051/api/events/batch',
      expect.objectContaining({
        method: 'POST',
        body: expect.stringContaining('"events"')
      })
    )
  })
})
```

#### 2. Real-time Metrics Flow

```typescript
// Test: WebSocket connection → Arrow IPC → Dashboard update
test('Connect to WebSocket and receive Arrow metrics', async () => {
  render(<App />)
  
  // Verify WebSocket connection established
  await waitFor(() => {
    expect(mockWebSocket.send).toHaveBeenCalled()
  })
  
  // Simulate Arrow TableRecordBatch from server
  mockWebSocket.onmessage({
    data: arrowTableBuffer
  })
  
  // Verify metrics render
  expect(screen.getByText(/Total Sessions/i)).toBeInTheDocument()
})
```

#### 3. Session Management Flow

```typescript
// Test: Session creation → Timeout → Auto-renewal
test('Session extends on click activity', async () => {
  const { rerender } = render(<App />)
  
  // Get initial session ID
  const session1 = sessionStorage.getItem('clickstream_session_id')
  
  // Wait 25 minutes (less than 30min timeout)
  jest.useFakeTimers()
  jest.advanceTimersByTime(25 * 60 * 1000)
  
  // Simulate user click
  fireEvent.click(screen.getByRole('button'))
  
  // Session should still be same (renewed)
  const session2 = sessionStorage.getItem('clickstream_session_id')
  expect(session2).toBe(session1)
  
  // Wait another 25 minutes (total 50 > 30 timeout)
  jest.advanceTimersByTime(25 * 60 * 1000)
  
  // Next click should create new session
  fireEvent.click(screen.getByRole('button'))
  const session3 = sessionStorage.getItem('clickstream_session_id')
  expect(session3).not.toBe(session1)
})
```

#### 4. Kafka Message Verification

```bash
# Verify events reached Kafka
docker exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9056 \
  --topic clickstream-events \
  --from-beginning \
  --max-messages 10 \
  --property print.key=true
```

Expected output:
```json
sess-xyz-789  {"schemaVersion":"1.0","eventId":"550e8400...","userId":"user-abc-123",...}
```

---

## 🔄 Data Paths

### Real-time Path (WebSocket + Arrow)

```
User opens Dashboard
    ↓
RealtimeContext connects to ws://localhost:9052
    ↓
Server sends Arrow IPC TableRecordBatches
    ↓
useRealtimeMetrics hook parses Arrow data
    ↓
RealtimeDashboard renders metric cards
    ↓
Auto-reconnect on disconnect (exponential backoff)
```

### Historical Path (REST API)

```
User navigates to Sessions page
    ↓
TanStack Query fires GET /api/analytics/sessions
    ↓
Response cached (5min stale time)
    ↓
SessionTable renders rows from JSON data
    ↓
Pagination: load next batch on scroll/button click
```

### Event Tracking Path

```
User clicks button / navigates page
    ↓
trackEvent() called with event details
    ↓
Event added to queue
    ↓
Queue size ≥ 10 OR 2 seconds elapsed?
    ↓ YES: Send batch
POST /api/events/batch → Ingestion API → Kafka
    ↓ NO: Wait
```

---

## 🛠️ Development Commands

| Command | Purpose |
|---------|---------|
| `npm install` | Install dependencies |
| `npm run dev` | Start dev server (port 3000) |
| `npm test` | Run tests in watch mode |
| `npm run build` | Production build (vite build) |
| `npm run lint` | Run ESLint |
| `npm run preview` | Preview production build locally |

---

## 📝 Code Standards

### Component Naming
- **Atoms:** Descriptive UI name (Button, Badge, Spinner)
- **Molecules:** Feature + role (MetricCard, SessionRow)
- **Organisms:** Feature + type (RealtimeDashboard, SessionTable)
- **Pages:** Feature + "Page" (DashboardPage, SessionsPage)

### Event Tracking
- Track at **Organism level and above** (not atoms/molecules)
- Always include `sessionId` and `userId`
- Use descriptive `targetElement` (use element ID or semantic name)

### Proxy Configuration (Vite)

All API requests automatically proxied during dev:

```
/api/events → http://localhost:9051/api/events
/ws → ws://localhost:9052
```

---

## 🐛 Troubleshooting

| Issue | Solution |
|-------|----------|
| Port 3000 already in use | Change in vite.config.ts: `port: 3001` |
| WebSocket connection fails | Verify realtime-analytics running on port 9052 |
| Events not batching | Check event queue size and timeout logic in TrackingContext |
| TanStack Query cache not clearing | Verify `staleTime` and `cacheTime` settings |
| TypeScript errors | Run `npm install` and check tsconfig.json |

---

## 📚 Additional Resources

- [ React 19 Docs](https://react.dev)
- [TanStack Query Docs](https://tanstack.com/query/latest)
- [Apache Arrow JS](https://arrow.apache.org/docs/js/)
- [Vite Documentation](https://vite.dev)
- [Vitest Documentation](https://vitest.dev)

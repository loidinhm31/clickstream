# Frontend Integration Testing Guide

This document focuses on frontend-to-service integration patterns and test scenarios.

For the supported live-stack Playwright workflow, use:

- [E2E Testing Guide](./e2e-testing.md)
- `bash scripts/run-e2e.sh`

That wrapper now verifies infrastructure, starts missing app services, applies the Raw Archiver flush override, and handles the Playwright browser fallback used on Fedora hosts.

## Prerequisites

Before running integration tests, **all backend services must be running:**

```bash
# Terminal 1: Start Docker services
cd /path/to/clickstream
docker compose up -d

# Terminal 2: Verify services
docker compose ps
# kafka RUNNING :9056
# mongodb RUNNING :9055
# redis RUNNING :6379
```

## Required Backend Services

| Service | Port | Purpose | Health Check |
|---------|------|---------|--------------|
| Kafka | 9056 | Event message broker | `/opt/kafka/bin/kafka-topics.sh --list` |
| MongoDB | 9055 | Session/page aggregates | `mongo --eval "db.adminCommand('ping')"` |
| Ingestion API | 9051 | Event ingestion REST API | `curl http://localhost:9051/health` |
| Real-time Analytics | 9052 | WebSocket metrics stream | `curl -i -N -H "Connection: Upgrade" ws://localhost:9052` |

---

## Test Scenarios

### 1. Event Submission Flow

**Test:** User clicks → Event batching → API submission → Kafka

**Steps:**

```typescript
// frontend/src/__tests__/integration/event-submission.spec.tsx
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { App } from '../../App'

describe('Event Submission - Integration', () => {
  it('should batch 10 clicks and send to API', async () => {
    const { rerender } = render(<App />)
    
    // Simulate 10 button clicks
    const clickButton = screen.getByRole('button', { name: /click me/i })
    for (let i = 0; i < 10; i++) {
      fireEvent.click(clickButton)
    }
    
    // Verify batch POST to /api/events/batch
    await waitFor(() => {
      const networkRequests = (window as any).networkLog || []
      const batchRequest = networkRequests.find(
        (req: any) => req.method === 'POST' && req.url.includes('/api/events/batch')
      )
      
      expect(batchRequest).toBeDefined()
      expect(batchRequest.body.events.length).toBe(10)
    }, { timeout: 3000 })
  })

  it('should send batch after 2 seconds timeout', async () => {
    jest.useFakeTimers()
    render(<App />)
    
    // Click button once
    fireEvent.click(screen.getByRole('button'))
    
    // Wait 2 seconds
    jest.advanceTimersByTime(2000)
    
    // Verify batch sent (even though < 10 events)
    await waitFor(() => {
      expect(mockFetch).toHaveBeenCalledWith(
        expect.stringContaining('/api/events/batch'),
        expect.objectContaining({ method: 'POST' })
      )
    })
    
    jest.useRealTimers()
  })
})
```

**Verification with Kafka:**

```bash
# Check if events reached Kafka
docker exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9056 \
  --topic clickstream-events \
  --from-beginning \
  --max-messages 10

# Expected output: JSON events with CLICK eventType
```

---

### 2. Real-time Metrics Flow

**Test:** WebSocket connection → Arrow IPC → Dashboard updates

**Steps:**

```typescript
// frontend/src/__tests__/integration/realtime-metrics.spec.tsx
import { render, screen, waitFor } from '@testing-library/react'
import { App } from '../../App'

describe('Real-time Metrics - Integration', () => {
  it('should connect to WebSocket and display metrics', async () => {
    // Mock WebSocket server response with Arrow data
    const mockArrowBatch = createArrowTableBatch([
      { name: 'total_sessions', value: 1024 },
      { name: 'total_clicks', value: 52341 },
    ])
    
    // Create mock WebSocket
    const mockWs = {
      send: jest.fn(),
      close: jest.fn(),
      addEventListener: jest.fn((event, handler) => {
        if (event === 'open') {
          setTimeout(() => handler(new Event('open')), 100)
        }
        if (event === 'message') {
          setTimeout(() => handler({ data: mockArrowBatch }), 200)
        }
      }),
    }
    
    global.WebSocket = jest.fn(() => mockWs) as any
    
    render(<App />)
    
    // Wait for WebSocket to connect and data to render
    await waitFor(() => {
      expect(screen.getByText(/total Sessions/i)).toBeInTheDocument()
      expect(screen.getByText('1024')).toBeInTheDocument()
    })
  })

  it('should auto-reconnect on WebSocket disconnect', async () => {
    const mockWs = {
      send: jest.fn(),
      close: jest.fn(),
      addEventListener: jest.fn(),
    }
    
    global.WebSocket = jest.fn(() => mockWs) as any
    
    render(<App />)
    
    // Simulate disconnect
    const closeHandler = mockWs.addEventListener.mock.calls.find(
      ([event]) => event === 'close'
    )?.[1]
    
    closeHandler?.()
    
    // Verify reconnect attempt (exponential backoff: 1s, 2s, 4s, ...)
    await waitFor(() => {
      expect(global.WebSocket).toHaveBeenCalledTimes(2)
    }, { timeout: 5000 })
  })

  it('should display connection status indicator', async () => {
    render(<App />)
    
    // Should show "disconnected" indicator initially
    let statusDot = screen.getByTestId('status-dot')
    expect(statusDot).toHaveClass('status-disconnected')
    
    // Simulate WebSocket connection
    const onOpen = (window as any).mockWsHandlers.open
    onOpen?.()
    
    // Should show "connected" indicator
    await waitFor(() => {
      expect(statusDot).toHaveClass('status-connected')
    })
  })
})
```

**Verification:**

```bash
# Start real-time analytics server (if not already running)
cd realtime-analytics
python -m realtime_analytics.server

# Monitor WebSocket traffic with browser DevTools:
# 1. Open http://localhost:3000 in browser
# 2. DevTools → Network → Filter for "WebSocket"
# 3. Click "ws://localhost:9052"
# 4. Messages tab shows Arrow binary data
```

---

### 3. Session Management Flow

**Test:** Session creation → Timeout → Auto-renewal → Expiration

**Steps:**

```typescript
// frontend/src/__tests__/integration/session-management.spec.tsx
import { render, fireEvent, waitFor } from '@testing-library/react'
import { App } from '../../App'

describe('Session Management - Integration', () => {
  beforeEach(() => {
    sessionStorage.clear()
    jest.useFakeTimers()
  })

  afterEach(() => {
    jest.useRealTimers()
  })

  it('should create session on first app load', () => {
    render(<App />)
    
    const sessionId = sessionStorage.getItem('clickstream_session_id')
    const timestamp = sessionStorage.getItem('clickstream_session_timestamp')
    
    expect(sessionId).toBeTruthy()
    expect(sessionId).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/) // UUID v4
    expect(timestamp).toBeTruthy()
  })

  it('should extend session on click activity', () => {
    const { rerender } = render(<App />)
    
    const sessionId1 = sessionStorage.getItem('clickstream_session_id')
    const timestamp1 = parseInt(sessionStorage.getItem('clickstream_session_timestamp') || '0')
    
    // Advance time by 25 minutes (less than 30 timeout)
    jest.advanceTimersByTime(25 * 60 * 1000)
    
    // Simulate user click
    fireEvent.click(screen.getByRole('button'))
    
    const timestamp2 = parseInt(sessionStorage.getItem('clickstream_session_timestamp') || '0')
    
    // Session should be renewed (same ID, but newer timestamp)
    expect(sessionStorage.getItem('clickstream_session_id')).toBe(sessionId1)
    expect(timestamp2).toBeGreaterThan(timestamp1)
  })

  it('should expire session after 30 minutes of inactivity', () => {
    render(<App />)
    
    const sessionId1 = sessionStorage.getItem('clickstream_session_id')
    
    // Advance time by 31 minutes (exceeds 30-minute timeout)
    jest.advanceTimersByTime(31 * 60 * 1000)
    
    // Simulate user click after timeout
    fireEvent.click(screen.getByRole('button'))
    
    const sessionId2 = sessionStorage.getItem('clickstream_session_id')
    
    // Session should be new (different ID)
    expect(sessionId2).not.toBe(sessionId1)
  })

  it('should persist session across page refreshes', async () => {
    const { unmount } = render(<App />)
    
    const sessionId1 = sessionStorage.getItem('clickstream_session_id')
    
    // Unmount component (simulate page refresh)
    unmount()
    
    // Re-mount component
    render(<App />)
    
    const sessionId2 = sessionStorage.getItem('clickstream_session_id')
    
    // Session should persist (same ID)
    expect(sessionId2).toBe(sessionId1)
  })
})
```

**Manual Verification:**

```javascript
// Browser console
sessionStorage.getItem('clickstream_session_id')  // UUID
sessionStorage.getItem('clickstream_session_timestamp')  // Milliseconds

// Should update on activity
document.querySelector('button').click()
// Timestamp should refresh
sessionStorage.getItem('clickstream_session_timestamp')  // Newer value

// After 30+ minutes of inactivity
// sessionId should be new on next activity
```

---

### 4. Historical Data Fetching

**Test:** REST API → TanStack Query caching → Table render

**Steps:**

```typescript
// frontend/src/__tests__/integration/historical-data.spec.tsx
import { render, screen, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { SessionsPage } from '../../components/pages/SessionsPage'

describe('Historical Data Fetching - Integration', () => {
  let queryClient: QueryClient

  beforeEach(() => {
    queryClient = new QueryClient({
      defaultOptions: {
        queries: { retry: false },
      },
    })
  })

  it('should fetch sessions from API and render table', async () => {
    // Mock fetch response
    global.fetch = jest.fn(() =>
      Promise.resolve({
        ok: true,
        json: () => Promise.resolve({
          data: [
            { id: 'sess-1', userId: 'user-1', duration: 300, pageCount: 5 },
            { id: 'sess-2', userId: 'user-2', duration: 450, pageCount: 8 },
          ],
          total: 2,
        }),
      })
    )

    render(
      <QueryClientProvider client={queryClient}>
        <SessionsPage />
      </QueryClientProvider>
    )

    // Wait for data to load
    await waitFor(() => {
      expect(screen.getByText('sess-1')).toBeInTheDocument()
      expect(screen.getByText('sess-2')).toBeInTheDocument()
    })

    // Verify API was called with correct endpoint
    expect(global.fetch).toHaveBeenCalledWith(
      '/api/analytics/sessions?page=0&size=10',
      expect.any(Object)
    )
  })

  it('should cache data for 5 minutes', async () => {
    let callCount = 0
    global.fetch = jest.fn(() => {
      callCount++
      return Promise.resolve({
        ok: true,
        json: () => Promise.resolve({ data: [], total: 0 }),
      })
    })

    const { rerender } = render(
      <QueryClientProvider client={queryClient}>
        <SessionsPage />
      </QueryClientProvider>
    )

    await waitFor(() => expect(callCount).toBe(1))

    // Re-render within cache window (5 min)
    rerender(
      <QueryClientProvider client={queryClient}>
        <SessionsPage />
      </QueryClientProvider>
    )

    // Should not call API again (using cache)
    await waitFor(() => expect(callCount).toBe(1))
  })
})
```

**Verification with Network Tab:**

```bash
# 1. Open browser DevTools
# 2. Go to Network tab
# 3. Navigate to Sessions page
# 4. Should see GET request to /api/analytics/sessions?page=0&size=10
# 5. Response should contain session array with pagination metadata
```

---

## Test Data Setup

### Sample Events

```json
{
  "events": [
    {
      "schemaVersion": "1.0",
      "eventId": "550e8400-e29b-41d4-a716-446655440000",
      "userId": "test-user-1",
      "sessionId": "test-session-1",
      "eventType": "CLICK",
      "targetElement": "button#test-button",
      "pageUrl": "http://localhost:3000/dashboard",
      "referrerUrl": "http://localhost:3000/",
      "timestamp": 1712678400000,
      "userAgent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
      "metadata": {
        "x": 450,
        "y": 320,
        "elementText": "Submit"
      }
    }
  ]
}
```

### MongoDB Test Collections

```javascript
// Create test sessions
db.sessions.insertMany([
  {
    _id: ObjectId(),
    sessionId: "test-session-1",
    userId: "test-user-1",
    startTime: ISODate("2026-04-18T10:00:00Z"),
    endTime: ISODate("2026-04-18T10:05:00Z"),
    eventCount: 42,
    pages: ["dashboard", "sessions"]
  }
])

// Create test page metrics
db.pages.insertMany([
  {
    _id: ObjectId(),
    pageUrl: "http://localhost:3000/dashboard",
    viewCount: 1024,
    uniqueUsers: 342,
    avgDuration: 120
  }
])
```

---

## Running Integration Tests

### Prerequisites Check

```bash
# 1. Verify Kafka is running
docker exec kafka kafka-broker-api-versions.sh --bootstrap-server localhost:9056

# 2. Verify MongoDB is running
docker exec mongodb mongosh --eval "db.adminCommand('ping')"

# 3. Verify Ingestion API is running
curl http://localhost:9051/health

# 4. Verify Real-time Analytics is running
curl -v http://localhost:9052/health 2>&1 | grep -i "101\|upgrade"
```

### Run Tests

```bash
# Run all integration tests
npm test -- integration

# Run specific integration test
npm test -- event-submission.spec.tsx

# Run with coverage
npm test -- --coverage integration/

# Run in debug mode (Chrome DevTools)
node --inspect-brk node_modules/.bin/vitest run integration/
```

### Generate Test Report

```bash
# Generate HTML coverage report
npm test -- --coverage integration/ --coverage.reporter=html

# Open coverage report
open coverage/index.html  # macOS
start coverage/index.html  # Windows
```

---

## CI/CD Integration (GitHub Actions)

```yaml
# .github/workflows/test-frontend-integration.yml
name: Frontend Integration Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    
    services:
      kafka:
        image: confluentinc/cp-kafka:7.5.0
        options: >-
          --health-cmd "kafka-broker-api-versions --bootstrap-server localhost:9056"
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
        ports:
          - 9056:9056
      
      mongodb:
        image: mongo:7.0
        options: >-
          --health-cmd "mongosh --eval 'db.adminCommand(\"ping\")'"
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
        ports:
          - 9055:9055
    
    steps:
      - uses: actions/checkout@v3
      
      - name: Setup Node
        uses: actions/setup-node@v3
        with:
          node-version: '18'
          cache: 'npm'
      
      - name: Install dependencies
        run: npm install
      
      - name: Start Ingestion API
        run: |
          cd ../ingestion-api
          mvn spring-boot:run &
          sleep 10  # Wait for API to start
      
      - name: Start Real-time Analytics
        run: |
          cd ../realtime-analytics
          python -m realtime_analytics.server &
          sleep 5
      
      - name: Run integration tests
        run: npm test -- integration --coverage
      
      - name: Upload coverage
        uses: codecov/codecov-action@v3
        with:
          files: ./coverage/coverage-final.json
```

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| WebSocket connection fails | Verify realtime-analytics running on port 9052 |
| Events not batching | Check event queue logic, verify network isn't blocking requests |
| Session not extending | Verify sessionStorage accessible, check timestamp calculation |
| API returns 503 (Service Unavailable) | Verify Ingestion API running, check Kafka connection |
| Timeout errors in tests | Increase `waitFor` timeout or verify services are responsive |
| MongoDB connection refused | Verify MongoDB running: `docker compose ps` |

---

## Related Documentation

- [Event Tracking Guide](./event-tracking.md)
- [Configuration Guide](./configuration.md)
- [Component Architecture](./components.md)

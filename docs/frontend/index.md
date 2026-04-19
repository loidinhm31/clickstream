# Frontend Documentation

React 19 analytics dashboard with real-time WebSocket support and event tracking.

## Contents

- [Configuration Guide](./configuration.md) - Environment setup, Vite config, API endpoints
- [Component Architecture](./components.md) - Atomic design, component hierarchy, examples
- [Event Tracking](./event-tracking.md) - Session management, event batching, tracking patterns
- [Integration Testing](./integration-testing.md) - End-to-end testing, service verification
- [Performance Guide](./performance.md) - Optimization techniques, bundle size, caching strategies

## Quick Start

```bash
cd frontend
npm install
npm run dev  # http://localhost:3000
```

**Requires backend services:**
- Ingestion API: http://localhost:9051
- Real-time Analytics: ws://localhost:9052

## Project Statistics

| Metric | Value |
|--------|-------|
| Total Components | 24+ |
| Atoms | 6 |
| Molecules | 3 |
| Organisms | 4 |
| Pages | 4 |
| Test Coverage | 60%+ |

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│ React App (main + App.tsx)                                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│ Providers:                                                        │
│ ├─ TrackingContext (sessionId, userId, event tracking)         │
│ ├─ RealtimeContext (WebSocket, Arrow metrics)                  │
│ └─ QueryClientProvider (TanStack Query)                        │
│                                                                   │
│ Router (React Router DOM):                                       │
│ ├─ /dashboard → DashboardPage → RealtimeDashboard              │
│ ├─ /sessions → SessionsPage → SessionTable                     │
│ ├─ /pages → PagesAnalyticsPage → PageMetrics                   │
│ └─ /journeys → JourneysPage → JourneyVisualization            │
│                                                                   │
│ Shared Components:                                               │
│ ├─ NavigationBar (with event tracking)                         │
│ ├─ ErrorBoundary (error handling)                              │
│ └─ DashboardTemplate (layout)                                  │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
        ↓                           ↓
    API Calls                  WebSocket
    /api/analytics/*           ws://localhost:9052
    (REST + TanStack Query)    (Arrow IPC + RealtimeContext)
```

## Key Concepts

### 1. Atomic Design Pattern

- **Atoms:** Reusable UI elements (Button, Badge, MetricValue)
- **Molecules:** Combinations of atoms (MetricCard, SessionRow)
- **Organisms:** Self-contained features (RealtimeDashboard, SessionTable)
- **Templates:** Page layouts (DashboardTemplate)
- **Pages:** Routable components with tracking (DashboardPage)

### 2. Event Tracking

All user interactions tracked automatically:
- Events queued in TrackingContext
- Batched when 10 events collected OR 2 seconds elapsed
- Sent to POST /api/events/batch
- Published to Kafka → downstream processing

### 3. Real-time Metrics

WebSocket connection to Arrow Flight server:
- Connection established on app start
- Auto-reconnects with exponential backoff
- Arrow IPC data parsed and displayed
- Metrics update live in RealtimeDashboard

### 4. Session Management

```javascript
Session Timeout: 30 minutes
Auto-renewal: Extends on any activity (click, page view, scroll)
Storage: sessionStorage (browser memory)
```

## Related Documentation

- **Main README:** [General project overview](../../README.md)
- **Onboarding Guide:** [Phase 07 frontend setup](../../ONBOARDING.md#phase-7-react-frontend-development)
- **Architecture Design:** [System architecture docs](../system-architecture.md)
- **Code Standards:** [Frontend code standards](../code-standards.md)

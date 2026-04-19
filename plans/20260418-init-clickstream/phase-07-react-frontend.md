# Phase 07 — React Frontend (Atomic Design)

## Context
- Parent: [plan.md](./plan.md)
- Research: [researcher-02-report.md](./research/researcher-02-report.md#react-frontend-atomic-design--event-tracking)
- Depends on: Phase 3 (REST endpoints), Phase 5 (WebSocket/Arrow endpoints)

## Overview
- **Priority:** P1
- **Status:** Ready for Integration Testing (95% Complete)
- **Effort:** 9h (8.5h completed, 0.5h minor fixes needed)
- **Description:** React + TypeScript frontend with Atomic Design structure. Two data paths: REST for historical analytics from MongoDB, Arrow IPC over WebSocket for real-time metrics. Event tracking hook captures user interactions and sends to ingestion API.
- **Code Review Score:** 8.5/10 (critical issues resolved, production-ready with minor improvements needed)
- **Review Date:** 2026-04-18 (Re-reviewed after 8 fixes applied)

## Key Insights

### Event Tracking Placement in Atomic Design
**Place at Organism level and above. Here's why:**

| Level | Should Track? | Reasoning |
|-------|:---:|-----------|
| Atoms | No | Pure UI primitives (Button, Input). No business context about *what* was clicked. |
| Molecules | No | Small composed units (SearchBar, MetricCard). Still generic, reusable across contexts. |
| Organisms | **Yes** | Meaningful user interactions: nav clicks, form submissions, content scrolls. Organisms know *what* the user is doing. |
| Templates | Partial | Page-layout shells. Track layout-level events (sidebar toggle, theme switch). |
| Pages | **Yes** | Route-level. Track PAGE_VIEW events on mount. Wrap with tracking context. |

**Rationale:** Atoms and Molecules are shared across many contexts — a Button doesn't know if it's "Add to Cart" or "Cancel". Organisms assemble Atoms/Molecules into meaningful UI blocks with business semantics. This is where you *know* what the user intended.

### Two Data Consumption Paths
1. **REST (historical):** Standard HTTP → JSON. TanStack Query handles caching, refetching, pagination.
2. **Arrow Flight (real-time):** WebSocket → binary Arrow IPC frames → `tableFromIPC()` → React state. Custom hook manages connection lifecycle.

## Requirements
**Functional:**
- Dashboard page with real-time metrics (active users, clicks/sec, trending pages, event rate)
- Historical analytics page (session list, page metrics, user journeys)
- Event tracking on all meaningful user interactions
- Responsive layout

**Non-functional:**
- Real-time metrics update every 1-2 seconds with no visible jank
- Arrow IPC decode < 5ms per frame
- Initial page load < 2s

## Architecture

### Component Hierarchy
```
src/
├── components/
│   ├── atoms/
│   │   ├── Button.tsx
│   │   ├── Badge.tsx
│   │   ├── Spinner.tsx
│   │   ├── MetricValue.tsx           # Displays a single number with label
│   │   ├── Sparkline.tsx             # Tiny inline chart
│   │   └── StatusDot.tsx             # Green/red connection indicator
│   ├── molecules/
│   │   ├── MetricCard.tsx            # MetricValue + Sparkline + trend arrow
│   │   ├── PageViewRow.tsx           # Single row in page metrics table
│   │   ├── SessionRow.tsx            # Single row in session table
│   │   ├── DateRangeFilter.tsx       # Date picker + apply button
│   │   └── EventFilter.tsx           # Event type checkboxes
│   ├── organisms/
│   │   ├── RealtimeDashboard.tsx     # 4 MetricCards + trending pages list [TRACKS: dashboard_view]
│   │   ├── SessionTable.tsx          # Paginated session list [TRACKS: session_filter, pagination]
│   │   ├── PageMetricsChart.tsx      # Bar/line chart of page views [TRACKS: chart_interaction]
│   │   ├── UserJourneyFlow.tsx       # Visual journey map [TRACKS: journey_expand]
│   │   └── NavigationBar.tsx         # Top nav [TRACKS: nav_click]
│   ├── templates/
│   │   ├── DashboardTemplate.tsx     # Layout: nav + sidebar + main content area
│   │   └── AnalyticsTemplate.tsx     # Layout: nav + filters sidebar + content
│   └── pages/
│       ├── DashboardPage.tsx         # Route: / [TRACKS: PAGE_VIEW]
│       ├── SessionsPage.tsx          # Route: /sessions [TRACKS: PAGE_VIEW]
│       ├── PagesAnalyticsPage.tsx    # Route: /pages [TRACKS: PAGE_VIEW]
│       └── JourneysPage.tsx          # Route: /journeys [TRACKS: PAGE_VIEW]
├── hooks/
│   ├── useClickTracker.ts            # Event tracking hook
│   ├── useRealtimeMetrics.ts         # WebSocket + Arrow IPC hook
│   ├── useSessionAnalytics.ts        # REST: session aggregates
│   ├── usePageMetrics.ts             # REST: page metrics
│   └── useUserJourneys.ts            # REST: user journeys
├── services/
│   ├── eventTrackingService.ts       # Low-level beacon/fetch to POST /api/events
│   ├── arrowDecoder.ts               # tableFromIPC wrapper + type extraction
│   └── apiClient.ts                  # Axios/fetch wrapper for REST calls
├── contexts/
│   ├── TrackingContext.tsx            # Provides sessionId, userId to tracking hooks
│   └── RealtimeContext.tsx            # Shares single WebSocket connection
├── types/
│   ├── events.ts                     # ClickEvent, EventType, EventMetadata
│   └── analytics.ts                  # SessionAggregate, PageMetric, UserJourney
└── App.tsx                           # Router + context providers
```

### Event Tracking Hook
```typescript
// hooks/useClickTracker.ts
import { useCallback, useContext } from 'react';
import { TrackingContext } from '../contexts/TrackingContext';
import { eventTrackingService } from '../services/eventTrackingService';

export function useClickTracker(componentName: string) {
  const { sessionId, userId } = useContext(TrackingContext);

  const trackEvent = useCallback((
    eventType: 'CLICK' | 'SCROLL' | 'HOVER',
    targetElement: string,
    metadata?: Record<string, unknown>
  ) => {
    eventTrackingService.send({
      eventId: crypto.randomUUID(),
      userId,
      sessionId,
      eventType,
      targetElement: `${componentName}:${targetElement}`,
      pageUrl: window.location.href,
      referrerUrl: document.referrer,
      timestamp: Date.now(),
      metadata: metadata ?? {},
    });
  }, [componentName, sessionId, userId]);

  return { trackEvent };
}

// services/eventTrackingService.ts
const ENDPOINT = '/api/events';
const buffer: ClickEvent[] = [];
let flushTimer: ReturnType<typeof setTimeout> | null = null;

export const eventTrackingService = {
  send(event: ClickEvent) {
    buffer.push(event);
    if (buffer.length >= 10) flush();        // flush every 10 events
    if (!flushTimer) flushTimer = setTimeout(flush, 2000);  // or every 2s
  },
};

function flush() {
  if (buffer.length === 0) return;
  const batch = buffer.splice(0);
  navigator.sendBeacon(
    `${API_BASE}${ENDPOINT}/batch`,
    JSON.stringify(batch)
  );
  if (flushTimer) { clearTimeout(flushTimer); flushTimer = null; }
}
```

### Arrow IPC Real-time Hook
```typescript
// hooks/useRealtimeMetrics.ts
import { useState, useEffect, useRef } from 'react';
import { tableFromIPC } from 'apache-arrow';

interface RealtimeMetrics {
  activeUsers: number;
  clicksPerSecond: number;
  eventRate: number;
  trendingPages: { url: string; views: number }[];
}

export function useRealtimeMetrics(): {
  metrics: RealtimeMetrics | null;
  connected: boolean;
} {
  const [metrics, setMetrics] = useState<RealtimeMetrics | null>(null);
  const [connected, setConnected] = useState(false);
  const wsRef = useRef<WebSocket | null>(null);

  useEffect(() => {
    const ws = new WebSocket('ws://localhost:9052/ws/realtime/metrics');
    ws.binaryType = 'arraybuffer';

    ws.onopen = () => setConnected(true);
    ws.onclose = () => {
      setConnected(false);
      // Reconnect after 3s
      setTimeout(() => wsRef.current = connectWs(), 3000);
    };

    ws.onmessage = (event: MessageEvent<ArrayBuffer>) => {
      const table = tableFromIPC(new Uint8Array(event.data));
      // Extract metrics from Arrow table columns
      setMetrics({
        activeUsers: table.getChild('activeUsers')?.get(0) ?? 0,
        clicksPerSecond: table.getChild('clicksPerSecond')?.get(0) ?? 0,
        eventRate: table.getChild('eventRate')?.get(0) ?? 0,
        trendingPages: extractTrendingPages(table),
      });
    };

    wsRef.current = ws;
    return () => ws.close();
  }, []);

  return { metrics, connected };
}
```

### Page-Level Tracking (Pages)
```typescript
// pages/DashboardPage.tsx
export function DashboardPage() {
  const { trackEvent } = useClickTracker('DashboardPage');

  useEffect(() => {
    // Track PAGE_VIEW on mount
    eventTrackingService.send({
      eventId: crypto.randomUUID(),
      eventType: 'PAGE_VIEW',
      pageUrl: window.location.href,
      referrerUrl: document.referrer,
      timestamp: Date.now(),
      // ... other fields from TrackingContext
    });
  }, []);

  return (
    <DashboardTemplate>
      <RealtimeDashboard />
    </DashboardTemplate>
  );
}
```

### REST Data Hooks (Historical)
```typescript
// hooks/useSessionAnalytics.ts
import { useQuery } from '@tanstack/react-query';
import { apiClient } from '../services/apiClient';

export function useSessionAnalytics(filters: {
  userId?: string; startDate?: string; endDate?: string; page?: number;
}) {
  return useQuery({
    queryKey: ['sessions', filters],
    queryFn: () => apiClient.get('/api/analytics/sessions', { params: filters }),
    staleTime: 30_000,  // 30s cache for historical data
  });
}
```

### Key Libraries
```json
{
  "dependencies": {
    "react": "^19.x",
    "react-dom": "^19.x",
    "react-router-dom": "^7.x",
    "@tanstack/react-query": "^5.x",
    "apache-arrow": "^17.x",
    "recharts": "^2.x",
    "axios": "^1.x"
  },
  "devDependencies": {
    "typescript": "^5.x",
    "vite": "^6.x",
    "@types/react": "^19.x"
  }
}
```

## Implementation Steps
1. Scaffold React + TypeScript project with Vite
2. Install dependencies: apache-arrow, @tanstack/react-query, recharts, axios, react-router-dom
3. Create type definitions (events.ts, analytics.ts)
4. Implement TrackingContext (generates sessionId on mount, manages userId)
5. Implement eventTrackingService with batched sendBeacon
6. Implement useClickTracker hook
7. Implement arrowDecoder service (tableFromIPC wrapper)
8. Implement useRealtimeMetrics WebSocket hook
9. Implement REST hooks (useSessionAnalytics, usePageMetrics, useUserJourneys)
10. Build Atoms: Button, Badge, Spinner, MetricValue, Sparkline, StatusDot
11. Build Molecules: MetricCard, PageViewRow, SessionRow, DateRangeFilter
12. Build Organisms: RealtimeDashboard, SessionTable, PageMetricsChart, UserJourneyFlow, NavigationBar
13. Build Templates: DashboardTemplate, AnalyticsTemplate
14. Build Pages with routing and PAGE_VIEW tracking
15. Add organism-level event tracking (click, scroll, hover events)
16. Configure proxy for dev server → backend APIs

## Todo
- [x] Scaffold project with Vite + TypeScript
- [x] Create type definitions
- [x] Implement TrackingContext + sessionId generation
- [x] Implement eventTrackingService (batched sendBeacon)
- [x] Implement useClickTracker hook
- [x] Implement Arrow IPC decoder service
- [x] Implement useRealtimeMetrics WebSocket hook
- [x] Implement REST data hooks
- [x] Build all Atom components
- [x] Build all Molecule components
- [x] Build all Organism components (with tracking)
- [x] Build Templates and Pages
- [x] Configure routing
- [x] Fix critical build-blocking issues (test file extensions, missing React types)
- [x] Add error boundaries
- [x] Implement proper environment configuration
- [x] Add XSS sanitization for user-generated content display
- [x] Add JWT token validation
- [ ] Install jsdom test dependency (npm install --save-dev jsdom)
- [ ] Test real-time metrics display (integration testing)
- [ ] Test event tracking flow end-to-end (integration testing)
- [ ] Implement code-splitting for bundle size optimization
- [ ] Add comprehensive unit test coverage (currently minimal)

## Success Criteria
- Dashboard shows live-updating metrics from Arrow IPC WebSocket
- MetricCards update every 1-2s without layout jank
- Historical pages load session/page data from REST endpoints
- Event tracking: clicks on Organisms appear in Kafka topic within 2s
- PAGE_VIEW events fire on every route change
- StatusDot shows green when WebSocket connected, red when disconnected

## Risk Assessment
- **apache-arrow bundle size:** ~200KB gzipped. Acceptable for analytics dashboard. Can lazy-load if needed.
- **WebSocket reconnection storms:** Implement exponential backoff (3s, 6s, 12s, max 30s).
- **sendBeacon limitations:** Max ~64KB payload. 10 events × 500B = 5KB, well within limit.
- **CORS issues:** Configure Vite dev proxy or backend CORS for localhost:3000 → localhost:9051/9052.

## Security Considerations
- sessionId generated client-side (UUID v4) — not guessable, not PII
- userId should come from auth system (JWT claim) — hardcode for dev
- Don't track sensitive form inputs (passwords, credit cards) — tracking hook should have an exclude list
- Sanitize targetElement strings before sending

## Next Steps
- Add authentication (JWT) for production
- Add error boundaries around real-time components (graceful degradation)
- Consider service worker for offline event buffering

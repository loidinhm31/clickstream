# Phase 04: Frontend

Setup and integration of the React-based analytics dashboard.

## 1. Installation

Install dependencies using `pnpm`.

**Commands:**
```bash
cd frontend
pnpm install
```

## 2. Configuration

Ensure the frontend is configured to point to the correct API and WebSocket endpoints.
Check `frontend/src/services/apiClient.ts` or similar config files.

- **Ingestion API**: `http://localhost:9051`
- **Real-time WebSocket**: `ws://localhost:9052/ws/realtime/metrics`

## 3. Development Server

Start the Vite development server.

**Commands:**
```bash
cd frontend
pnpm dev
```

**Access**: `http://localhost:9054` (or as specified in `vite.config.ts`)

## 4. Verification

1. Open the dashboard.
2. Verify that historical metrics are loaded from the Ingestion API.
3. Verify that real-time charts are updating via WebSockets.
4. Perform some clicks on the "test" area of the dashboard and verify they appear in Kafka UI.

## Summary Checklist
- [ ] Dependencies installed
- [ ] Vite server running
- [ ] Dashboard accessible in browser
- [ ] Data flowing from backend to UI

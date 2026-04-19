import { createContext, useEffect, useRef, useState, ReactNode } from 'react';

interface RealtimeContextValue {
  ws: WebSocket | null;
  connected: boolean;
}

export const RealtimeContext = createContext<RealtimeContextValue>({
  ws: null,
  connected: false,
});

interface RealtimeProviderProps {
  children: ReactNode;
  wsUrl?: string;
}

const DEFAULT_WS_URL = import.meta.env.VITE_WS_URL ||
  `${window.location.protocol === 'https:' ? 'wss' : 'ws'}://${window.location.host}/ws/realtime/metrics`;

export function RealtimeProvider({
  children,
  wsUrl = DEFAULT_WS_URL
}: RealtimeProviderProps) {
  const [connected, setConnected] = useState(false);
  const wsRef = useRef<WebSocket | null>(null);
  const reconnectTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const reconnectDelayRef = useRef(3000);
  const activeRef = useRef(false);

  const connect = () => {
    if (!activeRef.current) return;
    try {
      const ws = new WebSocket(wsUrl);
      ws.binaryType = 'arraybuffer';

      ws.onopen = () => {
        // Guard against stale callbacks from a previous connection instance
        if (wsRef.current !== ws) return;
        setConnected(true);
        reconnectDelayRef.current = 3000;
      };

      ws.onclose = () => {
        // Guard against stale callbacks from a previous connection instance
        if (wsRef.current !== ws) return;
        setConnected(false);
        wsRef.current = null;

        if (!activeRef.current) return;

        // Exponential backoff: 3s, 6s, 12s, max 30s
        const delay = Math.min(reconnectDelayRef.current, 30000);
        reconnectDelayRef.current *= 2;

        reconnectTimeoutRef.current = setTimeout(() => {
          connect();
        }, delay);
      };

      // Do NOT call ws.close() here — the browser transitions the WebSocket to
      // CLOSING/CLOSED automatically after an error, and onclose will handle reconnection.
      // Calling close() explicitly generates a redundant onclose cycle.
      ws.onerror = () => {};

      wsRef.current = ws;
    } catch (error) {
      console.error('WebSocket connection failed:', error);
    }
  };

  useEffect(() => {
    activeRef.current = true;
    reconnectDelayRef.current = 3000;
    connect();

    return () => {
      activeRef.current = false;
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current);
        reconnectTimeoutRef.current = null;
      }
      if (wsRef.current) {
        const ws = wsRef.current;
        wsRef.current = null;
        // Null out handlers before closing so stale callbacks don't fire on an
        // intentionally-closed socket. This is necessary for React Strict Mode,
        // which unmounts then remounts every effect in development.
        ws.onopen = null;
        ws.onclose = null;
        ws.onerror = null;
        ws.close();
      }
    };
  }, [wsUrl]);

  return (
    <RealtimeContext.Provider value={{ ws: wsRef.current, connected }}>
      {children}
    </RealtimeContext.Provider>
  );
}

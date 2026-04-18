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

const DEFAULT_WS_URL = import.meta.env.VITE_WS_URL || 'ws://localhost:8082/ws/realtime/metrics';

export function RealtimeProvider({ 
  children, 
  wsUrl = DEFAULT_WS_URL 
}: RealtimeProviderProps) {
  const [connected, setConnected] = useState(false);
  const wsRef = useRef<WebSocket | null>(null);
  const reconnectTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const reconnectDelayRef = useRef(3000); // Start with 3s

  const connect = () => {
    try {
      const ws = new WebSocket(wsUrl);
      ws.binaryType = 'arraybuffer';

      ws.onopen = () => {
        setConnected(true);
        reconnectDelayRef.current = 3000; // Reset delay on successful connection
      };

      ws.onclose = () => {
        setConnected(false);
        wsRef.current = null;

        // Exponential backoff: 3s, 6s, 12s, max 30s
        const delay = Math.min(reconnectDelayRef.current, 30000);
        reconnectDelayRef.current *= 2;

        reconnectTimeoutRef.current = setTimeout(() => {
          connect();
        }, delay);
      };

      ws.onerror = () => {
        ws.close();
      };

      wsRef.current = ws;
    } catch (error) {
      console.error('WebSocket connection failed:', error);
    }
  };

  useEffect(() => {
    connect();

    return () => {
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current);
      }
      if (wsRef.current) {
        wsRef.current.close();
      }
    };
  }, [wsUrl]);

  return (
    <RealtimeContext.Provider value={{ ws: wsRef.current, connected }}>
      {children}
    </RealtimeContext.Provider>
  );
}

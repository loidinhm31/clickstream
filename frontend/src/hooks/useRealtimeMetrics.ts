import { useState, useEffect, useContext } from 'react';
import { RealtimeContext } from '../contexts/RealtimeContext';
import { decodeArrowMetrics, RealtimeMetrics } from '../services/arrowDecoder';

export function useRealtimeMetrics(): {
  metrics: RealtimeMetrics | null;
  connected: boolean;
  error: string | null;
} {
  const { ws, connected } = useContext(RealtimeContext);
  const [metrics, setMetrics] = useState<RealtimeMetrics | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!ws) return;

    const handleMessage = (event: MessageEvent<ArrayBuffer>) => {
      try {
        const decoded = decodeArrowMetrics(event.data);
        if (decoded) {
          setMetrics(decoded);
          setError(null);
        } else {
          setError('Failed to decode metrics');
        }
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Unknown error');
      }
    };

    ws.addEventListener('message', handleMessage);

    return () => {
      ws.removeEventListener('message', handleMessage);
    };
  }, [ws]);

  return { metrics, connected, error };
}

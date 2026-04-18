import { createContext, useEffect, useState, useMemo, ReactNode } from 'react';

interface TrackingContextValue {
  sessionId: string;
  userId: string;
}

export const TrackingContext = createContext<TrackingContextValue>({
  sessionId: '',
  userId: 'anonymous',
});

interface TrackingProviderProps {
  children: ReactNode;
  userId?: string;
}

const generateSessionId = (): string => {
  // UUID v4 generation
  return crypto.randomUUID();
};

const getOrCreateSessionId = (): string => {
  const SESSION_KEY = 'clickstream_session_id';
  const SESSION_DURATION = 30 * 60 * 1000; // 30 minutes
  const TIMESTAMP_KEY = 'clickstream_session_timestamp';

  const storedSessionId = sessionStorage.getItem(SESSION_KEY);
  const storedTimestamp = sessionStorage.getItem(TIMESTAMP_KEY);

  const now = Date.now();

  // Check if session is still valid
  if (storedSessionId && storedTimestamp) {
    const timestamp = parseInt(storedTimestamp, 10);
    if (now - timestamp < SESSION_DURATION) {
      // Update timestamp to extend session
      sessionStorage.setItem(TIMESTAMP_KEY, now.toString());
      return storedSessionId;
    }
  }

  // Create new session
  const newSessionId = generateSessionId();
  sessionStorage.setItem(SESSION_KEY, newSessionId);
  sessionStorage.setItem(TIMESTAMP_KEY, now.toString());

  return newSessionId;
};

export function TrackingProvider({ children, userId = 'anonymous' }: TrackingProviderProps) {
  const [sessionId] = useState<string>(() => getOrCreateSessionId());

  // Update session timestamp on activity
  useEffect(() => {
    const updateTimestamp = () => {
      sessionStorage.setItem('clickstream_session_timestamp', Date.now().toString());
    };

    // Update on user activity
    window.addEventListener('click', updateTimestamp);
    window.addEventListener('scroll', updateTimestamp);
    window.addEventListener('keydown', updateTimestamp);

    return () => {
      window.removeEventListener('click', updateTimestamp);
      window.removeEventListener('scroll', updateTimestamp);
      window.removeEventListener('keydown', updateTimestamp);
    };
  }, []);

  const value = useMemo(
    () => ({ sessionId, userId }),
    [sessionId, userId]
  );

  return (
    <TrackingContext.Provider value={value}>
      {children}
    </TrackingContext.Provider>
  );
}

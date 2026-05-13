import { createContext, useEffect, useState, useMemo, ReactNode } from 'react';
import { createUuid } from '../utils/uuid';

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
  return createUuid();
};

const getSessionValue = (key: string): string | null => {
  try {
    return sessionStorage.getItem(key);
  } catch {
    return null;
  }
};

const setSessionValue = (key: string, value: string): void => {
  try {
    sessionStorage.setItem(key, value);
  } catch {
    // Tracking must not block app boot when storage is restricted.
  }
};

const getOrCreateSessionId = (): string => {
  const SESSION_KEY = 'clickstream_session_id';
  const SESSION_DURATION = 30 * 60 * 1000; // 30 minutes
  const TIMESTAMP_KEY = 'clickstream_session_timestamp';

  const storedSessionId = getSessionValue(SESSION_KEY);
  const storedTimestamp = getSessionValue(TIMESTAMP_KEY);

  const now = Date.now();

  // Check if session is still valid
  if (storedSessionId && storedTimestamp) {
    const timestamp = parseInt(storedTimestamp, 10);
    if (now - timestamp < SESSION_DURATION) {
      // Update timestamp to extend session
      setSessionValue(TIMESTAMP_KEY, now.toString());
      return storedSessionId;
    }
  }

  // Create new session
  const newSessionId = generateSessionId();
  setSessionValue(SESSION_KEY, newSessionId);
  setSessionValue(TIMESTAMP_KEY, now.toString());

  return newSessionId;
};

export function TrackingProvider({ children, userId = 'anonymous' }: TrackingProviderProps) {
  const [sessionId] = useState<string>(() => getOrCreateSessionId());

  // Update session timestamp on activity
  useEffect(() => {
    const updateTimestamp = () => {
      setSessionValue('clickstream_session_timestamp', Date.now().toString());
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

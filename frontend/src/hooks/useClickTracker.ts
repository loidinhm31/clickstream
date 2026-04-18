import { useCallback, useContext } from 'react';
import { TrackingContext } from '../contexts/TrackingContext';
import { eventTrackingService } from '../services/eventTrackingService';
import { EventType, ClickEvent } from '../types/events';

export function useClickTracker(componentName: string) {
  const { sessionId, userId } = useContext(TrackingContext);

  const trackEvent = useCallback(
    (
      eventType: EventType,
      targetElement: string,
      metadata?: Record<string, unknown>
    ) => {
      const event: ClickEvent = {
        eventId: crypto.randomUUID(),
        userId,
        sessionId,
        eventType,
        targetElement: `${componentName}:${targetElement}`,
        pageUrl: window.location.href,
        referrerUrl: document.referrer || '',
        timestamp: Date.now(),
        metadata: {
          userAgent: navigator.userAgent,
          screenResolution: `${window.screen.width}x${window.screen.height}`,
          viewport: `${window.innerWidth}x${window.innerHeight}`,
          ...metadata,
        },
      };

      eventTrackingService.send(event);
    },
    [componentName, sessionId, userId]
  );

  const trackClick = useCallback(
    (targetElement: string, metadata?: Record<string, unknown>) => {
      trackEvent('CLICK', targetElement, metadata);
    },
    [trackEvent]
  );

  const trackPageView = useCallback(
    (metadata?: Record<string, unknown>) => {
      trackEvent('PAGE_VIEW', window.location.pathname, metadata);
    },
    [trackEvent]
  );

  const trackScroll = useCallback(
    (targetElement: string, metadata?: Record<string, unknown>) => {
      trackEvent('SCROLL', targetElement, metadata);
    },
    [trackEvent]
  );

  const trackHover = useCallback(
    (targetElement: string, metadata?: Record<string, unknown>) => {
      trackEvent('HOVER', targetElement, metadata);
    },
    [trackEvent]
  );

  return {
    trackEvent,
    trackClick,
    trackPageView,
    trackScroll,
    trackHover,
  };
}

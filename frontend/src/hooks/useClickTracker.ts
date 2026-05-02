import { useCallback, useContext, useMemo } from 'react';
import debounce from 'lodash.debounce';
import { TrackingContext } from '../contexts/TrackingContext';
import { eventTrackingService } from '../services/eventTrackingService';
import { EventMetadata, EventType, ClickEvent } from '../types/events';

export function useClickTracker(componentName: string) {
  const { sessionId, userId } = useContext(TrackingContext);

  const trackEvent = useCallback(
    (eventType: EventType, targetElement: string, metadata?: EventMetadata) => {
      const referrer = document.referrer;
      const validReferrer =
        referrer && (referrer.startsWith('http://') || referrer.startsWith('https://'))
          ? referrer
          : undefined;
      const event: ClickEvent = {
        eventId: crypto.randomUUID(),
        userId,
        sessionId,
        eventType,
        targetElement: `${componentName}:${targetElement}`,
        pageUrl: window.location.href,
        referrerUrl: validReferrer,
        timestamp: Date.now(),
        userAgent: navigator.userAgent,
        metadata: {
          viewportWidth: window.innerWidth,
          viewportHeight: window.innerHeight,
          ...metadata,
        },
      };

      eventTrackingService.send(event);
    },
    [componentName, sessionId, userId]
  );

  const debouncedTrackClick = useMemo(
    () => debounce((targetElement: string, metadata?: EventMetadata, mouseEvent?: MouseEvent) => {
      trackEvent('CLICK', targetElement, {
        x: mouseEvent?.clientX ?? 0,
        y: mouseEvent?.clientY ?? 0,
        ...metadata,
      });
    }, 200),
    [trackEvent]
  );

  const trackClick = useCallback(
    (targetElement: string, metadata?: EventMetadata, mouseEvent?: MouseEvent) => {
      // Security: Check for declarative sensitive attributes
      if (mouseEvent?.target instanceof HTMLElement) {
        const target = mouseEvent.target;
        if (target.getAttribute('data-track') === 'false' || 
            target.getAttribute('data-sensitive') === 'true') {
          return;
        }
      }
      // Pass synthetic event data since the original mouseEvent may be pooled or lost
      debouncedTrackClick(targetElement, metadata, mouseEvent);
    },
    [debouncedTrackClick]
  );

  const trackPageView = useCallback(
    (metadata?: EventMetadata) => {
      trackEvent('PAGE_VIEW', window.location.pathname, metadata);
    },
    [trackEvent]
  );

  // scrollDepth must be in range 0.0-1.0
  const trackScroll = useCallback(
    (targetElement: string, scrollDepth: number, metadata?: EventMetadata) => {
      trackEvent('SCROLL', targetElement, {
        scrollDepth: Math.min(1, Math.max(0, scrollDepth)),
        ...metadata,
      });
    },
    [trackEvent]
  );

  const trackHover = useCallback(
    (targetElement: string, durationMs: number, metadata?: EventMetadata) => {
      trackEvent('HOVER', targetElement, { durationMs, ...metadata });
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

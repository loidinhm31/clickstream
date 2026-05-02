import { ClickEvent } from '../types/events';
import DOMPurify from 'dompurify';

const API_BASE = import.meta.env.VITE_API_BASE_URL || '';
const BATCH_ENDPOINT = '/api/events/batch';

// Validate configuration
if (!import.meta.env.VITE_API_BASE_URL && import.meta.env.PROD) {
  console.error('VITE_API_BASE_URL not configured for production');
}

const BATCH_SIZE = 10;
const FLUSH_INTERVAL = 2000; // 2 seconds

class EventTrackingService {
  private buffer: ClickEvent[] = [];
  private flushTimer: ReturnType<typeof setTimeout> | null = null;
  private excludeElements = new Set(['password', 'creditCard', 'ssn', 'secret']);

  send(event: ClickEvent): void {
    // Security: Sanitize targetElement to prevent XSS
    const sanitizedEvent = {
      ...event,
      targetElement: this.sanitizeTargetElement(event.targetElement),
    };

    // Don't track sensitive elements
    if (this.isSensitiveElement(sanitizedEvent.targetElement)) {
      return;
    }

    this.buffer.push(sanitizedEvent);

    // Flush if buffer is full
    if (this.buffer.length >= BATCH_SIZE) {
      this.flush();
    } else if (!this.flushTimer) {
      // Schedule flush
      this.flushTimer = setTimeout(() => this.flush(), FLUSH_INTERVAL);
    }
  }

  private sanitizeTargetElement(element: string): string {
    // Security: Use DOMPurify for robust XSS protection
    // We sanitize as text to strip any HTML structures while keeping selectors
    return DOMPurify.sanitize(element, { 
      ALLOWED_TAGS: [], 
      ALLOWED_ATTR: [] 
    }).substring(0, 200);
  }

  private isSensitiveElement(element: string): boolean {
    const lowerElement = element.toLowerCase();
    return Array.from(this.excludeElements).some(
      (sensitive) => lowerElement.includes(sensitive)
    );
  }

  private flush(): void {
    if (this.buffer.length === 0) return;

    const batch = this.buffer.splice(0);

    // Clear timer
    if (this.flushTimer) {
      clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }

    // Send batch using sendBeacon for reliability
    // Use Blob to set Content-Type: application/json (plain string defaults to text/plain)
    const payload = new Blob([JSON.stringify(batch)], { type: 'application/json' });
    const success = navigator.sendBeacon(
      `${API_BASE}${BATCH_ENDPOINT}`,
      payload
    );

    // Fallback to fetch if sendBeacon fails
    if (!success) {
      fetch(`${API_BASE}${BATCH_ENDPOINT}`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(batch),
        keepalive: true,
      }).catch((error) => {
        console.error('Failed to send events:', error);
      });
    }
  }

  // Flush on page unload
  flushOnUnload(): void {
    this.flush();
  }
}

export const eventTrackingService = new EventTrackingService();

// Flush events before page unload
if (typeof window !== 'undefined') {
  window.addEventListener('beforeunload', () => {
    eventTrackingService.flushOnUnload();
  });
}

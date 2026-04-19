// Type definitions for clickstream events matching backend schema

export type EventType = 'CLICK' | 'PAGE_VIEW' | 'SCROLL' | 'HOVER';

export interface EventMetadata {
  readonly x?: number;           // CLICK: mouse X coordinate
  readonly y?: number;           // CLICK: mouse Y coordinate
  readonly scrollDepth?: number; // SCROLL: 0.0-1.0
  readonly viewportWidth?: number;
  readonly viewportHeight?: number;
  readonly elementText?: string;
  readonly durationMs?: number;  // HOVER: hover duration in milliseconds
}

export interface ClickEvent {
  readonly eventId: string;
  readonly userId: string;
  readonly sessionId: string;
  readonly eventType: EventType;
  readonly targetElement: string;
  readonly pageUrl: string;
  readonly referrerUrl?: string; // null/undefined for direct navigation
  readonly timestamp: number;
  readonly userAgent?: string;
  readonly metadata?: EventMetadata;
}

export interface EventBatch {
  readonly events: ClickEvent[];
}

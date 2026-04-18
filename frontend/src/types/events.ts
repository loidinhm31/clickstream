// Type definitions for clickstream events matching backend schema

export type EventType = 'CLICK' | 'PAGE_VIEW' | 'SCROLL' | 'HOVER';

// Allowed primitive types for custom metadata
type MetadataValue = string | number | boolean | null;

export interface EventMetadata {
  readonly userAgent?: string;
  readonly screenResolution?: string;
  readonly viewport?: string;
  readonly deviceType?: string;
  readonly osName?: string;
  readonly browserName?: string;
  readonly timestamp?: number;
  readonly version?: string;
  readonly customData?: Record<string, MetadataValue>;
}

export interface ClickEvent {
  readonly eventId: string;
  readonly userId: string;
  readonly sessionId: string;
  readonly eventType: EventType;
  readonly targetElement: string;
  readonly pageUrl: string;
  readonly referrerUrl: string;
  readonly timestamp: number;
  readonly metadata?: EventMetadata;
}

export interface EventBatch {
  readonly events: ClickEvent[];
}

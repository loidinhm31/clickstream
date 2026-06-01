// Type definitions for analytics data from MongoDB
// Aligned with Spark ETL output schema (Phase 4)

export interface SessionAggregate {
  readonly sessionId: string;
  readonly userId: string;
  readonly windowStart: string; // ISO-8601 from Spring
  readonly windowEnd: string;
  readonly durationMs: number;
  readonly pageViewCount: number;
  readonly clickCount: number;
  readonly scrollEvents: number;
  readonly uniquePages: string[];
  readonly entryPage: string;
  readonly exitPage: string;
  readonly bounced: boolean;
}

export interface PageMetric {
  readonly pageUrl: string;
  readonly windowStart: string;
  readonly windowEnd: string;
  readonly totalViews: number;
  readonly uniqueVisitors: number;
  readonly clickCount: number;
  readonly avgScrollDepth: number;
  readonly bounceRate: number;
}

export interface UserJourney {
  readonly userId: string;
  readonly sessionId: string;
  readonly windowStart: string;
  readonly windowEnd: string;
  readonly orderedPages: OrderedPage[];
  readonly totalSessionDuration: number;
}

export interface OrderedPage {
  readonly pageUrl: string;
  readonly timestamp: number;
  readonly clicksOnPage: number;
}

export interface FilterParams {
  readonly userId?: string;
  readonly startDate?: string;
  readonly endDate?: string;
  readonly page?: number;
  readonly limit?: number;
}

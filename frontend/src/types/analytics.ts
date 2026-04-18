// Type definitions for analytics data from MongoDB

export interface SessionAggregate {
  readonly sessionId: string;
  readonly userId: string;
  readonly windowStart: number;
  readonly windowEnd: number;
  readonly totalClicks: number;
  readonly totalPageViews: number;
  readonly totalScrolls: number;
  readonly totalHovers: number;
  readonly uniquePages: number;
  readonly avgEventRate: number;
  readonly firstEventTime: number;
  readonly lastEventTime: number;
}

export interface PageMetric {
  readonly pageUrl: string;
  readonly totalViews: number;
  readonly uniqueUsers: number;
  readonly avgTimeOnPage: number;
  readonly bounceRate: number;
  readonly windowStart: number;
  readonly windowEnd: number;
}

export interface UserJourney {
  readonly sessionId: string;
  readonly userId: string;
  readonly eventSequence: JourneyEvent[];
  readonly startTime: number;
  readonly endTime: number;
  readonly totalEvents: number;
}

export interface JourneyEvent {
  readonly eventType: string;
  readonly targetElement: string;
  readonly pageUrl: string;
  readonly timestamp: number;
  readonly sequenceNum: number;
}

export interface FilterParams {
  readonly userId?: string;
  readonly startDate?: string;
  readonly endDate?: string;
  readonly page?: number;
  readonly limit?: number;
}

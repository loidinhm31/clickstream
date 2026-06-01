package com.clickstream.controller;

import com.clickstream.model.PageMetric;
import com.clickstream.model.SessionAggregate;
import com.clickstream.model.UserJourney;
import com.clickstream.service.AnalyticsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Page;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * REST Controller for analytics queries.
 * 
 * Endpoints:
 * - GET /api/analytics/sessions - Query session aggregates
 * - GET /api/analytics/pages - Query page metrics
 * - GET /api/analytics/journeys/{userId} - Query user journeys
 * 
 * All list endpoints support pagination via query params (page, size)
 * Time-based queries support ISO-8601 timestamps (startTime, endTime)
 */
@RestController
@RequestMapping("/api/analytics")
@CrossOrigin(origins = "${clickstream.cors.allowed-origins:http://localhost:3000}")
public class AnalyticsController {

    private static final Logger logger = LoggerFactory.getLogger(AnalyticsController.class);

    private final AnalyticsService analyticsService;

    public AnalyticsController(AnalyticsService analyticsService) {
        this.analyticsService = analyticsService;
    }

    // ==================== Session Endpoints ====================

    /**
     * Get all sessions with pagination
     * 
     * @param page Page number (default 0)
     * @param size Page size (default 20)
     * @param userId Optional user ID filter
     * @param startTime Optional start time filter (ISO-8601)
     * @param endTime Optional end time filter (ISO-8601)
     */
    @GetMapping("/sessions")
    public ResponseEntity<Page<SessionAggregate>> getSessions(
            @RequestParam(value = "page", defaultValue = "0") @Min(0) int page,
            @RequestParam(value = "size", defaultValue = "20") @Min(1) @Max(100) int size,
            @RequestParam(value = "userId", required = false) String userId,
            @RequestParam(value = "startTime", required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant startTime,
            @RequestParam(value = "endTime", required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant endTime) {
        
        logger.debug("GET /sessions: page={}, size={}, userId={}, startTime={}, endTime={}", 
                page, size, userId, startTime, endTime);
        
        Page<SessionAggregate> sessions;
        
        if (userId != null && startTime != null && endTime != null) {
            sessions = analyticsService.getSessionsByUserAndTimeRange(userId, startTime, endTime, page, size);
        } else if (userId != null) {
            sessions = analyticsService.getSessionsByUser(userId, page, size);
        } else if (startTime != null && endTime != null) {
            sessions = analyticsService.getSessionsByTimeRange(startTime, endTime, page, size);
        } else {
            sessions = analyticsService.getAllSessions(page, size);
        }
        
        return ResponseEntity.ok(sessions);
    }

    /**
     * Get session by ID
     */
    @GetMapping("/sessions/{sessionId}")
    public ResponseEntity<SessionAggregate> getSessionById(@PathVariable(value = "sessionId") String sessionId) {
        logger.debug("GET /sessions/{}", sessionId);
        Optional<SessionAggregate> session = analyticsService.getSessionById(sessionId);
        return session.map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    /**
     * Get session count
     */
    @GetMapping("/sessions/count")
    public ResponseEntity<CountResponse> getSessionCount(
            @RequestParam(value = "userId", required = false) String userId) {
        long count = userId != null 
                ? analyticsService.getSessionCountByUser(userId)
                : analyticsService.getTotalSessionCount();
        return ResponseEntity.ok(new CountResponse(count));
    }

    // ==================== Page Endpoints ====================

    /**
     * Get page metrics with pagination
     * 
     * @param page Page number (default 0)
     * @param size Page size (default 20)
     * @param pageUrl Optional page URL filter
     * @param startTime Optional start time filter (ISO-8601)
     * @param endTime Optional end time filter (ISO-8601)
     */
    @GetMapping("/pages")
    public ResponseEntity<Page<PageMetric>> getPageMetrics(
            @RequestParam(value = "page", defaultValue = "0") @Min(0) int page,
            @RequestParam(value = "size", defaultValue = "20") @Min(1) @Max(100) int size,
            @RequestParam(value = "pageUrl", required = false) String pageUrl,
            @RequestParam(value = "startTime", required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant startTime,
            @RequestParam(value = "endTime", required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant endTime) {
        
        logger.debug("GET /pages: page={}, size={}, pageUrl={}, startTime={}, endTime={}", 
                page, size, pageUrl, startTime, endTime);
        
        Page<PageMetric> metrics;
        
        if (pageUrl != null && startTime != null && endTime != null) {
            metrics = analyticsService.getPageMetricsByUrlAndTimeRange(pageUrl, startTime, endTime, page, size);
        } else if (pageUrl != null) {
            metrics = analyticsService.getPageMetricsByUrl(pageUrl, page, size);
        } else if (startTime != null && endTime != null) {
            metrics = analyticsService.getPageMetricsByTimeRange(startTime, endTime, page, size);
        } else {
            metrics = analyticsService.getAllPageMetrics(page, size);
        }
        
        return ResponseEntity.ok(metrics);
    }

    /**
     * Get top pages by views
     */
    @GetMapping("/pages/top")
    public ResponseEntity<List<PageMetric>> getTopPages(
            @RequestParam(value = "startTime", required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant startTime,
            @RequestParam(value = "endTime", required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant endTime,
            @RequestParam(value = "limit", defaultValue = "10") @Min(1) @Max(100) int limit) {
        
        // Default to last 24 hours if no time range specified
        if (startTime == null) {
            startTime = Instant.now().minusSeconds(86400);
        }
        if (endTime == null) {
            endTime = Instant.now();
        }
        
        logger.debug("GET /pages/top: limit={}, startTime={}, endTime={}", limit, startTime, endTime);
        List<PageMetric> topPages = analyticsService.getTopPagesByViews(startTime, endTime, limit);
        return ResponseEntity.ok(topPages);
    }

    // ==================== Journey Endpoints ====================

    /**
     * Get user journeys
     */
    @GetMapping("/journeys/{userId}")
    public ResponseEntity<List<UserJourney>> getUserJourneys(@PathVariable(value = "userId") String userId) {
        logger.debug("GET /journeys/{}", userId);
        List<UserJourney> journeys = analyticsService.getUserJourneys(userId);
        return ResponseEntity.ok(journeys);
    }

    /**
     * Get journey by session ID
     */
    @GetMapping("/journeys/session/{sessionId}")
    public ResponseEntity<UserJourney> getJourneyBySession(@PathVariable(value = "sessionId") String sessionId) {
        logger.debug("GET /journeys/session/{}", sessionId);
        Optional<UserJourney> journey = analyticsService.getJourneyBySession(sessionId);
        return journey.map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    // ==================== Health Check ====================

    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("Analytics API is healthy");
    }

    /**
     * Response DTO for count endpoints
     */
    public static class CountResponse {
        private long count;

        public CountResponse(long count) {
            this.count = count;
        }

        public long getCount() { return count; }
        public void setCount(long count) { this.count = count; }
    }
}

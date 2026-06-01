package com.clickstream.service;

import com.clickstream.model.PageMetric;
import com.clickstream.model.SessionAggregate;
import com.clickstream.model.UserJourney;
import com.clickstream.repository.PageMetricRepository;
import com.clickstream.repository.SessionAggregateRepository;
import com.clickstream.repository.UserJourneyRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Analytics service for querying aggregated clickstream data from MongoDB.
 * 
 * Data is written by Spark ETL (Phase 4) and read by this service.
 * All queries support pagination, filtering, and date ranges.
 */
@Service
public class AnalyticsService {

    private static final Logger logger = LoggerFactory.getLogger(AnalyticsService.class);

    private final SessionAggregateRepository sessionRepository;
    private final PageMetricRepository pageRepository;
    private final UserJourneyRepository journeyRepository;

    public AnalyticsService(
            SessionAggregateRepository sessionRepository,
            PageMetricRepository pageRepository,
            UserJourneyRepository journeyRepository) {
        this.sessionRepository = sessionRepository;
        this.pageRepository = pageRepository;
        this.journeyRepository = journeyRepository;
    }

    // ==================== Session Analytics ====================

    /**
     * Get all sessions with pagination
     */
    public Page<SessionAggregate> getAllSessions(int page, int size) {
        logger.debug("Fetching all sessions: page={}, size={}", page, size);
        Pageable pageable = PageRequest.of(page, size, Sort.by("windowStart").descending());
        return sessionRepository.findAll(pageable);
    }

    /**
     * Get sessions for specific user
     */
    public Page<SessionAggregate> getSessionsByUser(String userId, int page, int size) {
        logger.debug("Fetching sessions for user: userId={}, page={}, size={}", userId, page, size);
        Pageable pageable = PageRequest.of(page, size, Sort.by("windowStart").descending());
        return sessionRepository.findByUserId(userId, pageable);
    }

    /**
     * Get sessions within time range
     */
    public Page<SessionAggregate> getSessionsByTimeRange(
            Instant startTime, Instant endTime, int page, int size) {
        logger.debug("Fetching sessions in time range: {} to {}", startTime, endTime);
        Pageable pageable = PageRequest.of(page, size, Sort.by("windowStart").descending());
        return sessionRepository.findByTimeRange(startTime, endTime, pageable);
    }

    /**
     * Get sessions for user within time range
     */
    public Page<SessionAggregate> getSessionsByUserAndTimeRange(
            String userId, Instant startTime, Instant endTime, int page, int size) {
        logger.debug("Fetching sessions for user in time range: userId={}, {} to {}", 
                userId, startTime, endTime);
        Pageable pageable = PageRequest.of(page, size, Sort.by("windowStart").descending());
        return sessionRepository.findByUserIdAndTimeRange(userId, startTime, endTime, pageable);
    }

    /**
     * Get session by ID
     */
    public Optional<SessionAggregate> getSessionById(String sessionId) {
        logger.debug("Fetching session by ID: {}", sessionId);
        return sessionRepository.findById(sessionId);
    }

    // ==================== Page Analytics ====================

    /**
     * Get all page metrics with pagination
     */
    public Page<PageMetric> getAllPageMetrics(int page, int size) {
        logger.debug("Fetching all page metrics: page={}, size={}", page, size);
        Pageable pageable = PageRequest.of(page, size, Sort.by("windowStart").descending());
        return pageRepository.findAll(pageable);
    }

    /**
     * Get metrics for specific page
     */
    public Page<PageMetric> getPageMetricsByUrl(String pageUrl, int page, int size) {
        logger.debug("Fetching metrics for page: {}", pageUrl);
        Pageable pageable = PageRequest.of(page, size, Sort.by("windowStart").descending());
        return pageRepository.findByPageUrl(pageUrl, pageable);
    }

    /**
     * Get page metrics within time range
     */
    public Page<PageMetric> getPageMetricsByTimeRange(
            Instant startTime, Instant endTime, int page, int size) {
        logger.debug("Fetching page metrics in time range: {} to {}", startTime, endTime);
        Pageable pageable = PageRequest.of(page, size, Sort.by("windowStart").descending());
        return pageRepository.findByTimeRange(startTime, endTime, pageable);
    }

    /**
     * Get page metrics for specific page within time range
     */
    public Page<PageMetric> getPageMetricsByUrlAndTimeRange(
            String pageUrl, Instant startTime, Instant endTime, int page, int size) {
        logger.debug("Fetching metrics for page in time range: {} from {} to {}", 
                pageUrl, startTime, endTime);
        Pageable pageable = PageRequest.of(page, size, Sort.by("windowStart").descending());
        return pageRepository.findByPageUrlAndTimeRange(pageUrl, startTime, endTime, pageable);
    }

    /**
     * Get top pages by views
     */
    public List<PageMetric> getTopPagesByViews(Instant startTime, Instant endTime, int limit) {
        logger.debug("Fetching top {} pages by views", limit);
        Pageable pageable = PageRequest.of(0, limit);
        return pageRepository.findTopPagesByViews(startTime, endTime, pageable);
    }

    // ==================== User Journey Analytics ====================

    /**
     * Get journey for specific user
     */
    public List<UserJourney> getUserJourneys(String userId) {
        logger.debug("Fetching journeys for user: {}", userId);
        return journeyRepository.findByUserIdOrderByWindowStartDesc(userId);
    }

    /**
     * Get journey by session ID
     */
    public Optional<UserJourney> getJourneyBySession(String sessionId) {
        logger.debug("Fetching journey for session: {}", sessionId);
        return journeyRepository.findBySessionId(sessionId);
    }

    // ==================== Statistics ====================

    /**
     * Get session count for user
     */
    public long getSessionCountByUser(String userId) {
        return sessionRepository.countByUserId(userId);
    }

    /**
     * Get total session count
     */
    public long getTotalSessionCount() {
        return sessionRepository.count();
    }

    /**
     * Get total page metric count
     */
    public long getTotalPageMetricCount() {
        return pageRepository.count();
    }
}

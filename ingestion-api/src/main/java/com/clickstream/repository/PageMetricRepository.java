package com.clickstream.repository;

import com.clickstream.model.PageMetric;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;

/**
 * MongoDB repository for PageMetric documents.
 * 
 * Provides query methods for page-level analytics.
 */
@Repository
public interface PageMetricRepository extends MongoRepository<PageMetric, String> {

    /**
     * Find metrics for specific page URL
     */
    Page<PageMetric> findByPageUrl(String pageUrl, Pageable pageable);

    /**
     * Find metrics for page within time range
     */
    @Query("{ 'pageUrl': ?0, 'windowStart': { $gte: ?1, $lte: ?2 } }")
    Page<PageMetric> findByPageUrlAndTimeRange(
            String pageUrl, 
            Instant startTime, 
            Instant endTime, 
            Pageable pageable);

    /**
     * Find metrics within time range (all pages)
     */
    @Query("{ 'windowStart': { $gte: ?0, $lte: ?1 } }")
    Page<PageMetric> findByTimeRange(
            Instant startTime, 
            Instant endTime, 
            Pageable pageable);

    /**
     * Find top pages by total views
     */
    @Query(value = "{ 'windowStart': { $gte: ?0, $lte: ?1 } }", sort = "{ 'totalViews': -1 }")
    List<PageMetric> findTopPagesByViews(Instant startTime, Instant endTime, Pageable pageable);
}

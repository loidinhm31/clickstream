package com.clickstream.repository;

import com.clickstream.model.SessionAggregate;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;

/**
 * MongoDB repository for SessionAggregate documents.
 * 
 * Provides query methods for session analytics with pagination and filtering.
 */
@Repository
public interface SessionAggregateRepository extends MongoRepository<SessionAggregate, String> {

    /**
     * Find sessions by userId with pagination
     */
    Page<SessionAggregate> findByUserId(String userId, Pageable pageable);

    /**
     * Find sessions by userId within time range
     */
    @Query("{ 'userId': ?0, 'windowStart': { $gte: ?1, $lte: ?2 } }")
    Page<SessionAggregate> findByUserIdAndTimeRange(
            String userId, 
            Instant startTime, 
            Instant endTime, 
            Pageable pageable);

    /**
     * Find sessions within time range (all users)
     */
    @Query("{ 'windowStart': { $gte: ?0, $lte: ?1 } }")
    Page<SessionAggregate> findByTimeRange(
            Instant startTime, 
            Instant endTime, 
            Pageable pageable);

    /**
     * Count sessions by userId
     */
    long countByUserId(String userId);

    /**
     * Find sessions with minimum duration
     */
    @Query("{ 'durationMs': { $gte: ?0 } }")
    List<SessionAggregate> findLongSessions(Long minDurationMs);
}

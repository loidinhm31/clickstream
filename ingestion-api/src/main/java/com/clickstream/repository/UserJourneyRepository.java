package com.clickstream.repository;

import com.clickstream.model.UserJourney;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * MongoDB repository for UserJourney documents.
 * 
 * Provides query methods for user journey analysis.
 */
@Repository
public interface UserJourneyRepository extends MongoRepository<UserJourney, String> {

    /**
     * Find all journeys for a specific user
     */
    List<UserJourney> findByUserId(String userId);

    /**
     * Find journey by sessionId
     */
    Optional<UserJourney> findBySessionId(String sessionId);

    /**
     * Find journeys by userId ordered by start time
     */
    List<UserJourney> findByUserIdOrderByWindowStartDesc(String userId);
}

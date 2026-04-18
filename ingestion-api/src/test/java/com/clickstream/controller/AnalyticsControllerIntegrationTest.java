package com.clickstream.controller;

import com.clickstream.model.PageMetric;
import com.clickstream.model.SessionAggregate;
import com.clickstream.model.UserJourney;
import com.clickstream.repository.PageMetricRepository;
import com.clickstream.repository.SessionAggregateRepository;
import com.clickstream.repository.UserJourneyRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * Integration tests for AnalyticsController.
 * Uses Testcontainers MongoDB to test analytics queries.
 */
@SpringBootTest
@AutoConfigureMockMvc
@Testcontainers
class AnalyticsControllerIntegrationTest {

    @Container
    static MongoDBContainer mongoDBContainer = new MongoDBContainer("mongo:7.0.11")
            .withExposedPorts(27017);

    @DynamicPropertySource
    static void setProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", mongoDBContainer::getReplicaSetUrl);
    }

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private SessionAggregateRepository sessionRepository;

    @Autowired
    private PageMetricRepository pageRepository;

    @Autowired
    private UserJourneyRepository journeyRepository;

    @BeforeEach
    void setUp() {
        // Clear collections
        sessionRepository.deleteAll();
        pageRepository.deleteAll();
        journeyRepository.deleteAll();
    }

    @Test
    void testGetSessions_ReturnsPagedResults() throws Exception {
        // Given - 3 sessions in DB
        sessionRepository.saveAll(Arrays.asList(
                createSession("session-1", "user-1", Instant.now().minusSeconds(3600)),
                createSession("session-2", "user-1", Instant.now().minusSeconds(1800)),
                createSession("session-3", "user-2", Instant.now().minusSeconds(900))
        ));

        // When - GET /api/analytics/sessions
        mockMvc.perform(get("/api/analytics/sessions")
                        .param("page", "0")
                        .param("size", "10"))
                // Then - returns paginated sessions
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content").isArray())
                .andExpect(jsonPath("$.content.length()").value(3))
                .andExpect(jsonPath("$.totalElements").value(3))
                .andExpect(jsonPath("$.size").value(10));
    }

    @Test
    void testGetSessionsByUserId_FiltersCorrectly() throws Exception {
        // Given - sessions for different users
        sessionRepository.saveAll(Arrays.asList(
                createSession("session-1", "user-alice", Instant.now()),
                createSession("session-2", "user-alice", Instant.now().minusSeconds(300)),
                createSession("session-3", "user-bob", Instant.now())
        ));

        // When - GET /api/analytics/sessions?userId=user-alice
        mockMvc.perform(get("/api/analytics/sessions")
                        .param("userId", "user-alice")
                        .param("page", "0")
                        .param("size", "10"))
                // Then - returns only user-alice sessions
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content.length()").value(2))
                .andExpect(jsonPath("$.content[0].userId").value("user-alice"))
                .andExpect(jsonPath("$.content[1].userId").value("user-alice"));
    }

    @Test
    void testGetSessionById_ReturnsSession() throws Exception {
        // Given - session in DB
        SessionAggregate session = sessionRepository.save(
                createSession("session-999", "user-test", Instant.now()));

        // When - GET /api/analytics/sessions/{id}
        mockMvc.perform(get("/api/analytics/sessions/" + session.getId()))
                // Then - returns session
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value("session-999"))
                .andExpect(jsonPath("$.userId").value("user-test"));
    }

    @Test
    void testGetSessionById_NotFound_Returns404() throws Exception {
        // When - GET non-existent session
        mockMvc.perform(get("/api/analytics/sessions/nonexistent-id"))
                // Then - 404 Not Found
                .andExpect(status().isNotFound());
    }

    @Test
    void testGetPageMetrics_ReturnsPagedResults() throws Exception {
        // Given - page metrics in DB
        pageRepository.saveAll(Arrays.asList(
                createPageMetric("/home", Instant.now().minusSeconds(3600), 100),
                createPageMetric("/about", Instant.now().minusSeconds(1800), 50),
                createPageMetric("/contact", Instant.now().minusSeconds(900), 25)
        ));

        // When - GET /api/analytics/pages
        mockMvc.perform(get("/api/analytics/pages")
                        .param("page", "0")
                        .param("size", "10"))
                // Then - returns paginated metrics
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content").isArray())
                .andExpect(jsonPath("$.content.length()").value(3));
    }

    @Test
    void testGetPageMetricsByUrl_FiltersCorrectly() throws Exception {
        // Given - metrics for different pages
        pageRepository.saveAll(Arrays.asList(
                createPageMetric("/product/123", Instant.now(), 200),
                createPageMetric("/product/123", Instant.now().minusSeconds(3600), 150),
                createPageMetric("/product/456", Instant.now(), 100)
        ));

        // When - GET /api/analytics/pages?pageUrl=/product/123
        mockMvc.perform(get("/api/analytics/pages")
                        .param("pageUrl", "/product/123")
                        .param("page", "0")
                        .param("size", "10"))
                // Then - returns only /product/123 metrics
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content.length()").value(2))
                .andExpect(jsonPath("$.content[0].pageUrl").value("/product/123"));
    }

    @Test
    void testGetTopPages_ReturnsTopByViews() throws Exception {
        // Given - pages with different view counts
        pageRepository.saveAll(Arrays.asList(
                createPageMetric("/popular", Instant.now(), 1000),
                createPageMetric("/medium", Instant.now(), 500),
                createPageMetric("/unpopular", Instant.now(), 50)
        ));

        // When - GET /api/analytics/pages/top?limit=2
        mockMvc.perform(get("/api/analytics/pages/top")
                        .param("limit", "2"))
                // Then - returns top 2 pages
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(2))
                .andExpect(jsonPath("$[0].pageUrl").value("/popular"))
                .andExpect(jsonPath("$[0].totalViews").value(1000));
    }

    @Test
    void testGetUserJourneys_ReturnsJourneys() throws Exception {
        // Given - journey in DB
        UserJourney journey = createJourney("user-charlie", "session-555");
        journeyRepository.save(journey);

        // When - GET /api/analytics/journeys/{userId}
        mockMvc.perform(get("/api/analytics/journeys/user-charlie"))
                // Then - returns journeys
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isArray())
                .andExpect(jsonPath("$.length()").value(1))
                .andExpect(jsonPath("$[0].userId").value("user-charlie"))
                .andExpect(jsonPath("$[0].sessionId").value("session-555"));
    }

    @Test
    void testGetJourneyBySession_ReturnsJourney() throws Exception {
        // Given - journey in DB
        UserJourney journey = createJourney("user-delta", "session-777");
        journeyRepository.save(journey);

        // When - GET /api/analytics/journeys/session/{sessionId}
        mockMvc.perform(get("/api/analytics/journeys/session/session-777"))
                // Then - returns journey
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.sessionId").value("session-777"))
                .andExpect(jsonPath("$.userId").value("user-delta"));
    }

    @Test
    void testAnalyticsHealthEndpoint_ReturnsOk() throws Exception {
        mockMvc.perform(get("/api/analytics/health"))
                .andExpect(status().isOk())
                .andExpect(content().string("Analytics API is healthy"));
    }

    @Test
    void testGetSessionCount_ReturnsCount() throws Exception {
        // Given - 5 sessions
        for (int i = 0; i < 5; i++) {
            sessionRepository.save(createSession("session-" + i, "user-test", Instant.now()));
        }

        // When - GET /api/analytics/sessions/count
        mockMvc.perform(get("/api/analytics/sessions/count"))
                // Then - returns count
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.count").value(5));
    }

    // Helper methods

    private SessionAggregate createSession(String sessionId, String userId, Instant startTime) {
        return new SessionAggregate(
                sessionId,
                userId,
                startTime,
                startTime.plusSeconds(600), // 10 min session
                600000L,
                15,
                5,
                3,
                4,
                3,
                "/home",
                "/checkout",
                4,
                Map.of("/home", 1, "/products", 2, "/cart", 1, "/checkout", 1),
                "Mozilla/5.0",
                "192.168.1.1"
        );
    }

    private PageMetric createPageMetric(String pageUrl, Instant windowStart, int totalViews) {
        return new PageMetric(
                pageUrl + ":" + windowStart.toEpochMilli(),
                pageUrl,
                windowStart,
                windowStart.plusSeconds(3600),
                totalViews,
                totalViews / 2,
                totalViews * 3,
                totalViews / 5,
                totalViews / 10,
                5000L,
                totalViews / 10,
                0.1
        );
    }

    private UserJourney createJourney(String userId, String sessionId) {
        List<UserJourney.PageVisit> sequence = Arrays.asList(
                new UserJourney.PageVisit("/home", Instant.now().minusSeconds(300), 60000L, 5),
                new UserJourney.PageVisit("/products", Instant.now().minusSeconds(240), 120000L, 8),
                new UserJourney.PageVisit("/checkout", Instant.now().minusSeconds(120), 180000L, 12)
        );

        return new UserJourney(
                userId + ":" + sessionId,
                userId,
                sessionId,
                Instant.now().minusSeconds(300),
                Instant.now(),
                sequence,
                360000L,
                3
        );
    }
}

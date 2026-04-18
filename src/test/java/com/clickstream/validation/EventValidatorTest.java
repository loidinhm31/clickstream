package com.clickstream.validation;

import com.clickstream.model.ClickEvent;
import com.clickstream.model.EventMetadata;
import com.clickstream.model.EventType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for EventValidator.
 * Tests validation rules, PII checks, event type consistency, XSS protection,
 * URL validation, field length limits, and thread safety.
 */
class EventValidatorTest {

    private EventValidator validator;
    private long validTimestamp;

    @BeforeEach
    void setUp() {
        validator = new EventValidator();
        validTimestamp = System.currentTimeMillis();
    }

    private ClickEvent.Builder validEventBuilder() {
        return ClickEvent.builder()
                .eventId("valid-event-123")
                .userId("user-456")
                .sessionId("sess-789")
                .eventType(EventType.PAGE_VIEW)
                .pageUrl("https://example.com/page")
                .timestamp(validTimestamp);
    }

    @Test
    @DisplayName("Valid event should pass validation")
    void testValidEvent() {
        ClickEvent event = validEventBuilder().build();
        List<String> errors = validator.validate(event);
        assertTrue(errors.isEmpty(), "Valid event should have no errors");
        assertTrue(validator.isValid(event));
    }

    @Test
    @DisplayName("Null event should fail validation")
    void testNullEvent() {
        List<String> errors = validator.validate(null);
        assertEquals(1, errors.size());
        assertTrue(errors.get(0).contains("Event cannot be null"));
    }

    @Test
    @DisplayName("Missing eventId should fail validation")
    void testMissingEventId() {
        ClickEvent event = validEventBuilder().eventId(null).build();
        List<String> errors = validator.validate(event);
        assertFalse(errors.isEmpty());
        assertTrue(errors.stream().anyMatch(e -> e.contains("eventId is required")));
    }

    @Test
    @DisplayName("Blank eventId should fail validation")
    void testBlankEventId() {
        ClickEvent event = validEventBuilder().eventId("   ").build();
        List<String> errors = validator.validate(event);
        assertFalse(errors.isEmpty());
        assertTrue(errors.stream().anyMatch(e -> e.contains("eventId is required")));
    }

    @Test
    @DisplayName("Missing userId should fail validation")
    void testMissingUserId() {
        ClickEvent event = validEventBuilder().userId(null).build();
        List<String> errors = validator.validate(event);
        assertTrue(errors.stream().anyMatch(e -> e.contains("userId is required")));
    }

    @Test
    @DisplayName("Missing sessionId should fail validation")
    void testMissingSessionId() {
        ClickEvent event = validEventBuilder().sessionId(null).build();
        List<String> errors = validator.validate(event);
        assertTrue(errors.stream().anyMatch(e -> e.contains("sessionId is required")));
    }

    @Test
    @DisplayName("Missing eventType should fail validation")
    void testMissingEventType() {
        ClickEvent event = validEventBuilder().eventType(null).build();
        List<String> errors = validator.validate(event);
        assertTrue(errors.stream().anyMatch(e -> e.contains("eventType is required")));
    }

    @Test
    @DisplayName("Missing pageUrl should fail validation")
    void testMissingPageUrl() {
        ClickEvent event = validEventBuilder().pageUrl(null).build();
        List<String> errors = validator.validate(event);
        assertTrue(errors.stream().anyMatch(e -> e.contains("pageUrl is required")));
    }

    @Test
    @DisplayName("Missing timestamp should fail validation")
    void testMissingTimestamp() {
        ClickEvent event = validEventBuilder().timestamp(null).build();
        List<String> errors = validator.validate(event);
        assertTrue(errors.stream().anyMatch(e -> e.contains("timestamp is required")));
    }

    @ParameterizedTest
    @ValueSource(longs = {0, -1, -1000})
    @DisplayName("Non-positive timestamp should fail validation")
    void testNonPositiveTimestamp(long timestamp) {
        ClickEvent event = validEventBuilder().timestamp(timestamp).build();
        List<String> errors = validator.validate(event);
        assertTrue(errors.stream().anyMatch(e -> e.contains("timestamp must be positive")));
    }

    @Test
    @DisplayName("Future timestamp (>5min) should fail validation")
    void testFutureTimestamp() {
        ClickEvent event = validEventBuilder()
                .timestamp(System.currentTimeMillis() + 600000) // 10 minutes
                .build();
        List<String> errors = validator.validate(event);
        assertTrue(errors.stream().anyMatch(e -> e.contains("in the future")));
    }

    @Test
    @DisplayName("Old timestamp (>24h) should fail validation")
    void testOldTimestamp() {
        ClickEvent event = validEventBuilder()
                .timestamp(System.currentTimeMillis() - 90000000) // 25 hours
                .build();
        List<String> errors = validator.validate(event);
        assertTrue(errors.stream().anyMatch(e -> e.contains("too old")));
    }

    @Test
    @DisplayName("CLICK event without targetElement should fail")
    void testClickEventWithoutTargetElement() {
        ClickEvent event = validEventBuilder()
                .eventType(EventType.CLICK)
                .build();
        List<String> errors = validator.validate(event);
        assertTrue(errors.stream().anyMatch(e -> e.contains("CLICK events must have targetElement")));
    }

    @Test
    @DisplayName("CLICK event without x,y coordinates should fail")
    void testClickEventWithoutCoordinates() {
        ClickEvent event = validEventBuilder()
                .eventType(EventType.CLICK)
                .targetElement("button#submit")
                .metadata(EventMetadata.builder().elementText("Submit").build())
                .build();
        List<String> errors = validator.validate(event);
        assertTrue(errors.stream().anyMatch(e -> e.contains("x,y coordinates")));
    }

    @Test
    @DisplayName("Valid CLICK event should pass")
    void testValidClickEvent() {
        ClickEvent event = validEventBuilder()
                .eventType(EventType.CLICK)
                .targetElement("button#submit")
                .metadata(EventMetadata.builder().x(100).y(200).elementText("Submit").build())
                .build();
        List<String> errors = validator.validate(event);
        assertTrue(errors.isEmpty(), "Valid CLICK event should pass: " + errors);
    }

    @Test
    @DisplayName("SCROLL event without scrollDepth should fail")
    void testScrollEventWithoutScrollDepth() {
        ClickEvent event = validEventBuilder()
                .eventType(EventType.SCROLL)
                .build();
        List<String> errors = validator.validate(event);
        assertTrue(errors.stream().anyMatch(e -> e.contains("SCROLL events must have scrollDepth")));
    }

    @ParameterizedTest
    @ValueSource(doubles = {-0.1, 1.1, 2.0})
    @DisplayName("SCROLL event with invalid scrollDepth range should fail")
    void testScrollEventInvalidDepth(double depth) {
        ClickEvent event = validEventBuilder()
                .eventType(EventType.SCROLL)
                .metadata(EventMetadata.builder().scrollDepth(depth).build())
                .build();
        List<String> errors = validator.validate(event);
        assertTrue(errors.stream().anyMatch(e -> e.contains("scrollDepth must be between 0.0 and 1.0")));
    }

    @Test
    @DisplayName("Valid SCROLL event should pass")
    void testValidScrollEvent() {
        ClickEvent event = validEventBuilder()
                .eventType(EventType.SCROLL)
                .metadata(EventMetadata.builder().scrollDepth(0.75).build())
                .build();
        List<String> errors = validator.validate(event);
        assertTrue(errors.isEmpty(), "Valid SCROLL event should pass: " + errors);
    }

    @Test
    @DisplayName("HOVER event without targetElement should fail")
    void testHoverEventWithoutTargetElement() {
        ClickEvent event = validEventBuilder()
                .eventType(EventType.HOVER)
                .build();
        List<String> errors = validator.validate(event);
        assertTrue(errors.stream().anyMatch(e -> e.contains("HOVER events must have targetElement")));
    }

    @Test
    @DisplayName("HOVER event without durationMs should fail")
    void testHoverEventWithoutDuration() {
        ClickEvent event = validEventBuilder()
                .eventType(EventType.HOVER)
                .targetElement("div.product")
                .metadata(EventMetadata.builder().build())
                .build();
        List<String> errors = validator.validate(event);
        assertTrue(errors.stream().anyMatch(e -> e.contains("HOVER events must have durationMs")));
    }

    @Test
    @DisplayName("Valid HOVER event should pass")
    void testValidHoverEvent() {
        ClickEvent event = validEventBuilder()
                .eventType(EventType.HOVER)
                .targetElement("div.product")
                .metadata(EventMetadata.builder().durationMs(1500L).build())
                .build();
        List<String> errors = validator.validate(event);
        assertTrue(errors.isEmpty(), "Valid HOVER event should pass: " + errors);
    }

    @Test
    @DisplayName("sessionId equals userId should fail (PII security)")
    void testSessionIdEqualsUserId() {
        ClickEvent event = validEventBuilder()
                .userId("same-id-123")
                .sessionId("same-id-123")
                .build();
        List<String> errors = validator.validate(event);
        assertTrue(errors.stream().anyMatch(e -> e.contains("sessionId must not equal userId")));
    }

    @ParameterizedTest
    @ValueSource(strings = {"Enter Password", "SSN: 123-45-6789", "Credit-Card Number", "PIN code"})
    @DisplayName("elementText with PII keywords should fail")
    void testElementTextWithPII(String piiText) {
        ClickEvent event = validEventBuilder()
                .eventType(EventType.CLICK)
                .targetElement("input#field")
                .metadata(EventMetadata.builder().x(100).y(200).elementText(piiText).build())
                .build();
        List<String> errors = validator.validate(event);
        assertTrue(errors.stream().anyMatch(e -> e.contains("PII")));
    }

    @Test
    @DisplayName("userAgent with IP address should fail")
    void testUserAgentWithIPAddress() {
        ClickEvent event = validEventBuilder()
                .userAgent("Mozilla/5.0 (192.168.1.100)")
                .build();
        List<String> errors = validator.validate(event);
        assertTrue(errors.stream().anyMatch(e -> e.contains("IP addresses")));
    }

    // NEW TESTS for improved validation

    @ParameterizedTest
    @ValueSource(strings = {"<script>alert('XSS')</script>", "javascript:alert(1)", "<iframe src='evil'>", "onerror=alert(1)"})
    @DisplayName("XSS patterns in targetElement should fail")
    void testXSSInTargetElement(String xssPayload) {
        ClickEvent event = validEventBuilder()
                .targetElement(xssPayload)
                .build();
        List<String> errors = validator.validate(event);
        assertTrue(errors.stream().anyMatch(e -> e.contains("XSS")), "Should detect XSS in targetElement");
    }

    @Test
    @DisplayName("Invalid URL format should fail")
    void testInvalidURLFormat() {
        ClickEvent event = validEventBuilder()
                .pageUrl("not-a-valid-url")
                .build();
        List<String> errors = validator.validate(event);
        assertFalse(errors.isEmpty(), "Should have validation errors for invalid URL");
        assertTrue(errors.stream().anyMatch(e -> e.toLowerCase().contains("url")), 
                "Should have URL-related error");
    }

    @Test
    @DisplayName("Non-HTTP URL scheme should fail")
    void testNonHTTPURLScheme() {
        ClickEvent event = validEventBuilder()
                .pageUrl("ftp://example.com")
                .build();
        List<String> errors = validator.validate(event);
        assertTrue(errors.stream().anyMatch(e -> e.contains("HTTP")));
    }

    @Test
    @DisplayName("Field length limits should be enforced")
    void testFieldLengthLimits() {
        String longUrl = "https://example.com/" + "a".repeat(3000);
        ClickEvent event = validEventBuilder()
                .pageUrl(longUrl)
                .build();
        List<String> errors = validator.validate(event);
        assertTrue(errors.stream().anyMatch(e -> e.contains("exceeds maximum length")));
    }

    @Test
    @DisplayName("Configurable validation windows should work")
    void testConfigurableValidationWindows() {
        EventValidator customValidator = new EventValidator(1000L, 1000L); // 1 second windows
        ClickEvent event = validEventBuilder()
                .timestamp(System.currentTimeMillis() - 2000) // 2 seconds ago
                .build();
        List<String> errors = customValidator.validate(event);
        assertTrue(errors.stream().anyMatch(e -> e.contains("too old")));
    }

    @Test
    @DisplayName("Builder concurrency test - thread safety")
    void testBuilderThreadSafety() throws InterruptedException {
        int threadCount = 10;
        CountDownLatch latch = new CountDownLatch(threadCount);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    ClickEvent.Builder builder = ClickEvent.builder();
                    ClickEvent event = builder
                            .eventId("event-" + threadId)
                            .userId("user-" + threadId)
                            .sessionId("sess-" + threadId)
                            .eventType(EventType.PAGE_VIEW)
                            .pageUrl("https://example.com/page-" + threadId)
                            .timestamp(System.currentTimeMillis())
                            .build();

                    // Verify each thread got its own event
                    assertEquals("event-" + threadId, event.getEventId());
                    assertEquals("user-" + threadId, event.getUserId());
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(5, TimeUnit.SECONDS), "All threads should complete");
        executor.shutdown();
    }

    @Test
    @DisplayName("Multiple validation errors should all be reported")
    void testMultipleErrors() {
        ClickEvent event = ClickEvent.builder().build(); // all fields null
        List<String> errors = validator.validate(event);
        assertTrue(errors.size() >= 6, "Should report all missing required fields");
    }

    @Test
    @DisplayName("Schema version should be included")
    void testSchemaVersionPresent() {
        ClickEvent event = validEventBuilder().build();
        assertNotNull(event.getSchemaVersion());
        assertEquals("1.0", event.getSchemaVersion());
    }

    @Test
    @DisplayName("Immutability test - equals/hashCode work correctly")
    void testEqualsHashCode() {
        ClickEvent event1 = validEventBuilder().eventId("same-id").build();
        ClickEvent event2 = validEventBuilder().eventId("same-id").build();
        ClickEvent event3 = validEventBuilder().eventId("different-id").build();

        assertEquals(event1, event2, "Events with same eventId should be equal");
        assertEquals(event1.hashCode(), event2.hashCode(), "Hash codes should match");
        assertNotEquals(event1, event3, "Events with different eventId should not be equal");
    }
}

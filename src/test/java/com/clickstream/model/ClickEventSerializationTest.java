package com.clickstream.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for ClickEvent serialization and deserialization.
 * Validates JSON schema compliance and Jackson annotation behavior.
 */
class ClickEventSerializationTest {

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
    }

    @Test
    @DisplayName("Should serialize ClickEvent to JSON matching schema")
    void testSerializeClickEvent() throws JsonProcessingException {
        // Arrange
        EventMetadata metadata = EventMetadata.builder()
                .x(450)
                .y(320)
                .scrollDepth(0.75)
                .viewportWidth(1920)
                .viewportHeight(1080)
                .elementText("Place Order")
                .build();

        ClickEvent event = ClickEvent.builder()
                .eventId("550e8400-e29b-41d4-a716-446655440000")
                .userId("user-abc-123")
                .sessionId("sess-xyz-789")
                .eventType(EventType.CLICK)
                .targetElement("button#submit-order")
                .pageUrl("https://app.example.com/checkout")
                .referrerUrl("https://app.example.com/cart")
                .timestamp(1712678400000L)
                .userAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
                .metadata(metadata)
                .build();

        // Act
        String json = objectMapper.writeValueAsString(event);

        // Assert
        assertNotNull(json);
        assertTrue(json.contains("\"eventId\":\"550e8400-e29b-41d4-a716-446655440000\""));
        assertTrue(json.contains("\"userId\":\"user-abc-123\""));
        assertTrue(json.contains("\"sessionId\":\"sess-xyz-789\""));
        assertTrue(json.contains("\"eventType\":\"CLICK\""));
        assertTrue(json.contains("\"targetElement\":\"button#submit-order\""));
        assertTrue(json.contains("\"pageUrl\":\"https://app.example.com/checkout\""));
        assertTrue(json.contains("\"timestamp\":1712678400000"));
        assertTrue(json.contains("\"metadata\":{"));
        assertTrue(json.contains("\"x\":450"));
        assertTrue(json.contains("\"elementText\":\"Place Order\""));
    }

    @Test
    @DisplayName("Should deserialize JSON to ClickEvent")
    void testDeserializeClickEvent() throws JsonProcessingException {
        // Arrange
        String json = """
                {
                  "eventId": "550e8400-e29b-41d4-a716-446655440000",
                  "userId": "user-abc-123",
                  "sessionId": "sess-xyz-789",
                  "eventType": "CLICK",
                  "targetElement": "button#submit-order",
                  "pageUrl": "https://app.example.com/checkout",
                  "referrerUrl": "https://app.example.com/cart",
                  "timestamp": 1712678400000,
                  "userAgent": "Mozilla/5.0...",
                  "metadata": {
                    "x": 450,
                    "y": 320,
                    "scrollDepth": 0.75,
                    "viewportWidth": 1920,
                    "viewportHeight": 1080,
                    "elementText": "Place Order"
                  }
                }
                """;

        // Act
        ClickEvent event = objectMapper.readValue(json, ClickEvent.class);

        // Assert
        assertNotNull(event);
        assertEquals("550e8400-e29b-41d4-a716-446655440000", event.getEventId());
        assertEquals("user-abc-123", event.getUserId());
        assertEquals("sess-xyz-789", event.getSessionId());
        assertEquals(EventType.CLICK, event.getEventType());
        assertEquals("button#submit-order", event.getTargetElement());
        assertEquals("https://app.example.com/checkout", event.getPageUrl());
        assertEquals("https://app.example.com/cart", event.getReferrerUrl());
        assertEquals(1712678400000L, event.getTimestamp());
        assertEquals("Mozilla/5.0...", event.getUserAgent());
        
        assertNotNull(event.getMetadata());
        assertEquals(450, event.getMetadata().getX());
        assertEquals(320, event.getMetadata().getY());
        assertEquals(0.75, event.getMetadata().getScrollDepth());
        assertEquals(1920, event.getMetadata().getViewportWidth());
        assertEquals(1080, event.getMetadata().getViewportHeight());
        assertEquals("Place Order", event.getMetadata().getElementText());
    }

    @Test
    @DisplayName("Should handle missing optional fields (metadata subfields)")
    void testDeserializeWithMissingOptionalFields() throws JsonProcessingException {
        // Arrange - PAGE_VIEW event with minimal metadata
        String json = """
                {
                  "eventId": "test-event-1",
                  "userId": "user-123",
                  "sessionId": "sess-456",
                  "eventType": "PAGE_VIEW",
                  "pageUrl": "https://app.example.com/home",
                  "timestamp": 1712678400000
                }
                """;

        // Act
        ClickEvent event = objectMapper.readValue(json, ClickEvent.class);

        // Assert
        assertNotNull(event);
        assertEquals("test-event-1", event.getEventId());
        assertEquals(EventType.PAGE_VIEW, event.getEventType());
        assertNull(event.getTargetElement());
        assertNull(event.getReferrerUrl());
        assertNull(event.getUserAgent());
        assertNull(event.getMetadata());
    }

    @Test
    @DisplayName("Should serialize and deserialize round-trip successfully")
    void testSerializationRoundTrip() throws JsonProcessingException {
        // Arrange
        ClickEvent original = ClickEvent.builder()
                .eventId("round-trip-test")
                .userId("user-999")
                .sessionId("sess-999")
                .eventType(EventType.SCROLL)
                .pageUrl("https://example.com/article")
                .timestamp(System.currentTimeMillis())
                .metadata(EventMetadata.builder()
                        .scrollDepth(0.5)
                        .viewportHeight(1080)
                        .build())
                .build();

        // Act
        String json = objectMapper.writeValueAsString(original);
        ClickEvent deserialized = objectMapper.readValue(json, ClickEvent.class);

        // Assert
        assertEquals(original.getEventId(), deserialized.getEventId());
        assertEquals(original.getUserId(), deserialized.getUserId());
        assertEquals(original.getSessionId(), deserialized.getSessionId());
        assertEquals(original.getEventType(), deserialized.getEventType());
        assertEquals(original.getPageUrl(), deserialized.getPageUrl());
        assertEquals(original.getTimestamp(), deserialized.getTimestamp());
        assertEquals(original.getMetadata().getScrollDepth(), deserialized.getMetadata().getScrollDepth());
    }

    @ParameterizedTest
    @EnumSource(EventType.class)
    @DisplayName("Should serialize all EventType enum values correctly")
    void testSerializeAllEventTypes(EventType eventType) throws JsonProcessingException {
        // Arrange
        ClickEvent event = ClickEvent.builder()
                .eventId("event-" + eventType.name())
                .userId("user-123")
                .sessionId("sess-123")
                .eventType(eventType)
                .pageUrl("https://example.com")
                .timestamp(System.currentTimeMillis())
                .build();

        // Act
        String json = objectMapper.writeValueAsString(event);

        // Assert
        assertNotNull(json);
        assertTrue(json.contains("\"eventType\":\"" + eventType.name() + "\""));
    }

    @Test
    @DisplayName("Should omit null metadata fields (JsonInclude.NON_NULL)")
    void testOmitNullMetadataFields() throws JsonProcessingException {
        // Arrange - metadata with only some fields set
        EventMetadata metadata = EventMetadata.builder()
                .x(100)
                .y(200)
                // scrollDepth, viewportWidth, viewportHeight, elementText, durationMs are null
                .build();

        // Act
        String json = objectMapper.writeValueAsString(metadata);

        // Assert
        assertTrue(json.contains("\"x\":100"));
        assertTrue(json.contains("\"y\":200"));
        assertFalse(json.contains("scrollDepth"), "Null scrollDepth should be omitted");
        assertFalse(json.contains("viewportWidth"), "Null viewportWidth should be omitted");
        assertFalse(json.contains("elementText"), "Null elementText should be omitted");
        assertFalse(json.contains("durationMs"), "Null durationMs should be omitted");
    }

    @Test
    @DisplayName("Should handle HOVER event with durationMs")
    void testHoverEventSerialization() throws JsonProcessingException {
        // Arrange
        ClickEvent event = ClickEvent.builder()
                .eventId("hover-event-1")
                .userId("user-123")
                .sessionId("sess-123")
                .eventType(EventType.HOVER)
                .targetElement("div.product-card")
                .pageUrl("https://shop.example.com/products")
                .timestamp(System.currentTimeMillis())
                .metadata(EventMetadata.builder()
                        .durationMs(1500L)
                        .build())
                .build();

        // Act
        String json = objectMapper.writeValueAsString(event);
        ClickEvent deserialized = objectMapper.readValue(json, ClickEvent.class);

        // Assert
        assertEquals(EventType.HOVER, deserialized.getEventType());
        assertEquals("div.product-card", deserialized.getTargetElement());
        assertNotNull(deserialized.getMetadata());
        assertEquals(1500L, deserialized.getMetadata().getDurationMs());
    }

    @Test
    @DisplayName("Should preserve timestamp as numeric (not ISO string)")
    void testTimestampFormat() throws JsonProcessingException {
        // Arrange
        ClickEvent event = ClickEvent.builder()
                .eventId("timestamp-test")
                .userId("user-123")
                .sessionId("sess-123")
                .eventType(EventType.PAGE_VIEW)
                .pageUrl("https://example.com")
                .timestamp(1712678400000L)
                .build();

        // Act
        String json = objectMapper.writeValueAsString(event);

        // Assert
        assertTrue(json.contains("\"timestamp\":1712678400000"));
        assertFalse(json.contains("\"timestamp\":\""), "Timestamp should be numeric, not string");
    }

    @Test
    @DisplayName("Builder should create valid event")
    void testBuilderPattern() {
        // Act
        ClickEvent event = ClickEvent.builder()
                .eventId("builder-test")
                .userId("user-builder")
                .sessionId("sess-builder")
                .eventType(EventType.CLICK)
                .pageUrl("https://example.com")
                .timestamp(System.currentTimeMillis())
                .build();

        // Assert
        assertNotNull(event);
        assertEquals("builder-test", event.getEventId());
        assertEquals("user-builder", event.getUserId());
        assertEquals(EventType.CLICK, event.getEventType());
    }
}

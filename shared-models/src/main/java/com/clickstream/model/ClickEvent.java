package com.clickstream.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Clickstream event model representing a single user interaction.
 * 
 * <p>This class defines the schema for events published to Kafka topic 'clickstream-events'.
 * All fields match the JSON schema specification from phase-02 requirements.
 * 
 * <p>Immutable design: All fields are final and set via constructor only.
 * This ensures thread-safety and prevents accidental modification of events.
 * 
 * <p>Schema:
 * <pre>
 * {
 *   "schemaVersion": "1.0",
 *   "eventId": "UUID",
 *   "userId": "user-abc-123",
 *   "sessionId": "sess-xyz-789",
 *   "eventType": "CLICK|PAGE_VIEW|SCROLL|HOVER",
 *   "targetElement": "button#submit-order",
 *   "pageUrl": "https://app.example.com/checkout",
 *   "referrerUrl": "https://app.example.com/cart",
 *   "timestamp": 1712678400000,
 *   "userAgent": "Mozilla/5.0...",
 *   "metadata": { ... }
 * }
 * </pre>
 */
public final class ClickEvent {

    /**
     * Schema version for compatibility tracking.
     */
    @JsonProperty("schemaVersion")
    private final String schemaVersion = "1.0";

    /**
     * Unique event identifier (UUID).
     * Required for deduplication and event tracking.
     */
    @JsonProperty("eventId")
    private final String eventId;

    /**
     * User identifier.
     * Used for analytics but NOT as partition key (to avoid hot partitions).
     */
    @JsonProperty("userId")
    private final String userId;

    /**
     * Session identifier (UUID).
     * Used as Kafka partition key to ensure session-level ordering.
     */
    @JsonProperty("sessionId")
    private final String sessionId;

    /**
     * Type of event (CLICK, PAGE_VIEW, SCROLL, HOVER).
     */
    @JsonProperty("eventType")
    private final EventType eventType;

    /**
     * CSS selector or ID of the target element (e.g., "button#submit-order").
     * Nullable for PAGE_VIEW events.
     */
    @JsonProperty("targetElement")
    private final String targetElement;

    /**
     * Full URL of the page where the event occurred.
     */
    @JsonProperty("pageUrl")
    private final String pageUrl;

    /**
     * Referrer URL (page the user came from).
     * Null for direct navigation.
     */
    @JsonProperty("referrerUrl")
    private final String referrerUrl;

    /**
     * Event timestamp in epoch milliseconds.
     * Must be positive and non-zero.
     */
    @JsonProperty("timestamp")
    @JsonFormat(shape = JsonFormat.Shape.NUMBER)
    private final Long timestamp;

    /**
     * Browser user agent string.
     * Used for device/browser analytics.
     */
    @JsonProperty("userAgent")
    private final String userAgent;

    /**
     * Event-specific metadata (coordinates, scroll depth, etc.).
     * Nullable - content varies by event type.
     */
    @JsonProperty("metadata")
    private final EventMetadata metadata;

    // Constructors

    /**
     * Default constructor for Jackson deserialization.
     */
    public ClickEvent() {
        this(null, null, null, null, null, null, null, null, null, null);
    }

    /**
     * All-args constructor for immutable instance creation and Jackson deserialization.
     * Use Builder for programmatic construction.
     */
    @JsonCreator
    public ClickEvent(
            @JsonProperty("eventId") String eventId,
            @JsonProperty("userId") String userId,
            @JsonProperty("sessionId") String sessionId,
            @JsonProperty("eventType") EventType eventType,
            @JsonProperty("targetElement") String targetElement,
            @JsonProperty("pageUrl") String pageUrl,
            @JsonProperty("referrerUrl") String referrerUrl,
            @JsonProperty("timestamp") Long timestamp,
            @JsonProperty("userAgent") String userAgent,
            @JsonProperty("metadata") EventMetadata metadata) {
        this.eventId = eventId;
        this.userId = userId;
        this.sessionId = sessionId;
        this.eventType = eventType;
        this.targetElement = targetElement;
        this.pageUrl = pageUrl;
        this.referrerUrl = referrerUrl;
        this.timestamp = timestamp;
        this.userAgent = userAgent;
        this.metadata = metadata;
    }

    // Getters only (no setters - immutable)

    public String getSchemaVersion() {
        return schemaVersion;
    }

    public String getEventId() {
        return eventId;
    }

    public String getUserId() {
        return userId;
    }

    public String getSessionId() {
        return sessionId;
    }

    public EventType getEventType() {
        return eventType;
    }

    public String getTargetElement() {
        return targetElement;
    }

    public String getPageUrl() {
        return pageUrl;
    }

    public String getReferrerUrl() {
        return referrerUrl;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public String getUserAgent() {
        return userAgent;
    }

    public EventMetadata getMetadata() {
        return metadata;
    }

    // Builder pattern for easier construction

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for ClickEvent construction.
     * Thread-safe: Creates new instance per build() call.
     */
    public static class Builder {
        private String eventId;
        private String userId;
        private String sessionId;
        private EventType eventType;
        private String targetElement;
        private String pageUrl;
        private String referrerUrl;
        private Long timestamp;
        private String userAgent;
        private EventMetadata metadata;

        public Builder eventId(String eventId) {
            this.eventId = eventId;
            return this;
        }

        public Builder userId(String userId) {
            this.userId = userId;
            return this;
        }

        public Builder sessionId(String sessionId) {
            this.sessionId = sessionId;
            return this;
        }

        public Builder eventType(EventType eventType) {
            this.eventType = eventType;
            return this;
        }

        public Builder targetElement(String targetElement) {
            this.targetElement = targetElement;
            return this;
        }

        public Builder pageUrl(String pageUrl) {
            this.pageUrl = pageUrl;
            return this;
        }

        public Builder referrerUrl(String referrerUrl) {
            this.referrerUrl = referrerUrl;
            return this;
        }

        public Builder timestamp(Long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder userAgent(String userAgent) {
            this.userAgent = userAgent;
            return this;
        }

        public Builder metadata(EventMetadata metadata) {
            this.metadata = metadata;
            return this;
        }

        public ClickEvent build() {
            return new ClickEvent(eventId, userId, sessionId, eventType,
                    targetElement, pageUrl, referrerUrl, timestamp, userAgent, metadata);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClickEvent that = (ClickEvent) o;
        return eventId != null ? eventId.equals(that.eventId) : that.eventId == null;
    }

    @Override
    public int hashCode() {
        return eventId != null ? eventId.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "ClickEvent{" +
                "schemaVersion='" + schemaVersion + '\'' +
                ", eventId='" + eventId + '\'' +
                ", userId='" + maskId(userId) + '\'' +
                ", sessionId='" + maskId(sessionId) + '\'' +
                ", eventType=" + eventType +
                ", targetElement='" + targetElement + '\'' +
                ", pageUrl='" + pageUrl + '\'' +
                ", referrerUrl='" + referrerUrl + '\'' +
                ", timestamp=" + timestamp +
                ", metadata=" + metadata +
                '}';
    }

    /**
     * Masks sensitive IDs for logging (shows first 4 chars + ***).
     */
    private static String maskId(String id) {
        if (id == null || id.length() <= 4) return "***";
        return id.substring(0, 4) + "***";
    }
}

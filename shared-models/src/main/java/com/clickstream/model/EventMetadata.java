package com.clickstream.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Metadata container for clickstream events.
 * 
 * <p>Contains event-type-specific data. Fields are nullable to accommodate
 * different event types (e.g., CLICK events have x/y, SCROLL events have scrollDepth).
 * 
 * <p>Immutable design: All fields are final and set via constructor only.
 * 
 * <p>Jackson will omit null fields during serialization when @JsonInclude(NON_NULL) is set.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class EventMetadata {
    
    /**
     * Mouse X coordinate (for CLICK events).
     */
    @JsonProperty("x")
    private final Integer x;

    /**
     * Mouse Y coordinate (for CLICK events).
     */
    @JsonProperty("y")
    private final Integer y;

    /**
     * Scroll depth percentage 0.0-1.0 (for SCROLL events).
     * Values: 0.25, 0.5, 0.75, 1.0
     */
    @JsonProperty("scrollDepth")
    private final Double scrollDepth;

    /**
     * Viewport width in pixels.
     */
    @JsonProperty("viewportWidth")
    private final Integer viewportWidth;

    /**
     * Viewport height in pixels.
     */
    @JsonProperty("viewportHeight")
    private final Integer viewportHeight;

    /**
     * Text content of the interacted element (for CLICK events).
     */
    @JsonProperty("elementText")
    private final String elementText;

    /**
     * Hover duration in milliseconds (for HOVER events).
     */
    @JsonProperty("durationMs")
    private final Long durationMs;

    // Constructors
    
    public EventMetadata() {
        this(null, null, null, null, null, null, null);
    }

    @JsonCreator
    public EventMetadata(
            @JsonProperty("x") Integer x,
            @JsonProperty("y") Integer y,
            @JsonProperty("scrollDepth") Double scrollDepth,
            @JsonProperty("viewportWidth") Integer viewportWidth,
            @JsonProperty("viewportHeight") Integer viewportHeight,
            @JsonProperty("elementText") String elementText,
            @JsonProperty("durationMs") Long durationMs) {
        this.x = x;
        this.y = y;
        this.scrollDepth = scrollDepth;
        this.viewportWidth = viewportWidth;
        this.viewportHeight = viewportHeight;
        this.elementText = elementText;
        this.durationMs = durationMs;
    }

    // Getters only (no setters - immutable)

    public Integer getX() {
        return x;
    }

    public Integer getY() {
        return y;
    }

    public Double getScrollDepth() {
        return scrollDepth;
    }

    public Integer getViewportWidth() {
        return viewportWidth;
    }

    public Integer getViewportHeight() {
        return viewportHeight;
    }

    public String getElementText() {
        return elementText;
    }

    public Long getDurationMs() {
        return durationMs;
    }

    // Builder pattern for easier construction

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for EventMetadata construction.
     * Thread-safe: Creates new instance per build() call.
     */
    public static class Builder {
        private Integer x;
        private Integer y;
        private Double scrollDepth;
        private Integer viewportWidth;
        private Integer viewportHeight;
        private String elementText;
        private Long durationMs;

        public Builder x(Integer x) {
            this.x = x;
            return this;
        }

        public Builder y(Integer y) {
            this.y = y;
            return this;
        }

        public Builder scrollDepth(Double scrollDepth) {
            this.scrollDepth = scrollDepth;
            return this;
        }

        public Builder viewportWidth(Integer viewportWidth) {
            this.viewportWidth = viewportWidth;
            return this;
        }

        public Builder viewportHeight(Integer viewportHeight) {
            this.viewportHeight = viewportHeight;
            return this;
        }

        public Builder elementText(String elementText) {
            this.elementText = elementText;
            return this;
        }

        public Builder durationMs(Long durationMs) {
            this.durationMs = durationMs;
            return this;
        }

        public EventMetadata build() {
            return new EventMetadata(x, y, scrollDepth, viewportWidth, viewportHeight, elementText, durationMs);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EventMetadata that = (EventMetadata) o;
        if (x != null ? !x.equals(that.x) : that.x != null) return false;
        if (y != null ? !y.equals(that.y) : that.y != null) return false;
        if (scrollDepth != null ? !scrollDepth.equals(that.scrollDepth) : that.scrollDepth != null) return false;
        if (viewportWidth != null ? !viewportWidth.equals(that.viewportWidth) : that.viewportWidth != null) return false;
        if (viewportHeight != null ? !viewportHeight.equals(that.viewportHeight) : that.viewportHeight != null) return false;
        if (elementText != null ? !elementText.equals(that.elementText) : that.elementText != null) return false;
        return durationMs != null ? durationMs.equals(that.durationMs) : that.durationMs == null;
    }

    @Override
    public int hashCode() {
        int result = x != null ? x.hashCode() : 0;
        result = 31 * result + (y != null ? y.hashCode() : 0);
        result = 31 * result + (scrollDepth != null ? scrollDepth.hashCode() : 0);
        result = 31 * result + (viewportWidth != null ? viewportWidth.hashCode() : 0);
        result = 31 * result + (viewportHeight != null ? viewportHeight.hashCode() : 0);
        result = 31 * result + (elementText != null ? elementText.hashCode() : 0);
        result = 31 * result + (durationMs != null ? durationMs.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "EventMetadata{" +
                "x=" + x +
                ", y=" + y +
                ", scrollDepth=" + scrollDepth +
                ", viewportWidth=" + viewportWidth +
                ", viewportHeight=" + viewportHeight +
                ", elementText='" + elementText + '\'' +
                ", durationMs=" + durationMs +
                '}';
    }
}

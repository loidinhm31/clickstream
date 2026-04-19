package com.clickstream.validation;

import com.clickstream.model.ClickEvent;
import com.clickstream.model.EventType;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Validator for ClickEvent instances.
 * 
 * <p>Performs business-level validation beyond basic bean validation annotations.
 * Checks for event type consistency, PII exposure, input sanitization, URL format,
 * and field length limits.
 * 
 * <p>Note: This is a plain Java class without Spring annotations to keep shared-models
 * framework-agnostic. Services using Spring should register this as a bean in their
 * configuration.
 */
public class EventValidator {

    // Pre-compiled regex patterns for performance
    private static final Pattern PII_PATTERN = Pattern.compile(
            "(?i)\\b(password|passwd|pwd|ssn|social.?security|credit.?card|cvv|pin)\\b");
    // Negative lookbehind for '/' to avoid matching browser version strings like Chrome/120.0.0.0
    private static final Pattern IP_ADDRESS_PATTERN = Pattern.compile(
            "(?<!/)\\b\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\b");
    private static final Pattern XSS_PATTERN = Pattern.compile(
            "(?i)<script|javascript:|onerror=|onclick=|<iframe");

    // Field length limits (to prevent exceeding Kafka max.message.bytes)
    private static final int MAX_ID_LENGTH = 255;
    private static final int MAX_URL_LENGTH = 2048;
    private static final int MAX_ELEMENT_SELECTOR_LENGTH = 512;
    private static final int MAX_USER_AGENT_LENGTH = 8192;
    private static final int MAX_ELEMENT_TEXT_LENGTH = 1024;

    // Configurable validation windows
    private final long maxEventAgeMs;
    private final long maxFutureDriftMs;

    /**
     * Default constructor with standard validation windows.
     * - Events must be within 24 hours old
     * - Events can be up to 5 minutes in the future (clock skew tolerance)
     */
    public EventValidator() {
        this(86400000L, 300000L);  // 24 hours, 5 minutes
    }

    /**
     * Constructor with configurable validation windows.
     * 
     * @param maxEventAgeMs Maximum age of events in milliseconds
     * @param maxFutureDriftMs Maximum future drift tolerance in milliseconds
     */
    public EventValidator(long maxEventAgeMs, long maxFutureDriftMs) {
        this.maxEventAgeMs = maxEventAgeMs;
        this.maxFutureDriftMs = maxFutureDriftMs;
    }

    /**
     * Validates a ClickEvent and returns a list of validation errors.
     * 
     * @param event the event to validate
     * @return list of validation error messages (empty if valid)
     */
    public List<String> validate(ClickEvent event) {
        List<String> errors = new ArrayList<>();

        if (event == null) {
            errors.add("Event cannot be null");
            return errors;
        }

        // Basic null and length checks
        validateRequiredFields(event, errors);
        validateFieldLengths(event, errors);

        // Security checks
        validateNoXSS(event, errors);
        validateNoPII(event, errors);

        // Format validation
        if (event.getTimestamp() != null) {
            validateTimestamp(event.getTimestamp(), errors);
        }
        if (event.getPageUrl() != null) {
            validateURL(event.getPageUrl(), "pageUrl", errors);
        }
        if (event.getReferrerUrl() != null && !event.getReferrerUrl().isBlank()) {
            validateURL(event.getReferrerUrl(), "referrerUrl", errors);
        }

        // Event type specific validation
        if (event.getEventType() != null) {
            validateEventTypeConsistency(event, errors);
        }

        return errors;
    }

    private void validateRequiredFields(ClickEvent event, List<String> errors) {
        if (event.getEventId() == null || event.getEventId().isBlank()) {
            errors.add("eventId is required and cannot be blank");
        }
        if (event.getUserId() == null || event.getUserId().isBlank()) {
            errors.add("userId is required and cannot be blank");
        }
        if (event.getSessionId() == null || event.getSessionId().isBlank()) {
            errors.add("sessionId is required and cannot be blank");
        }
        if (event.getEventType() == null) {
            errors.add("eventType is required");
        }
        if (event.getPageUrl() == null || event.getPageUrl().isBlank()) {
            errors.add("pageUrl is required and cannot be blank");
        }
        if (event.getTimestamp() == null) {
            errors.add("timestamp is required");
        }
    }

    private void validateFieldLengths(ClickEvent event, List<String> errors) {
        if (event.getEventId() != null && event.getEventId().length() > MAX_ID_LENGTH) {
            errors.add(String.format("eventId exceeds maximum length of %d characters", MAX_ID_LENGTH));
        }
        if (event.getUserId() != null && event.getUserId().length() > MAX_ID_LENGTH) {
            errors.add(String.format("userId exceeds maximum length of %d characters", MAX_ID_LENGTH));
        }
        if (event.getSessionId() != null && event.getSessionId().length() > MAX_ID_LENGTH) {
            errors.add(String.format("sessionId exceeds maximum length of %d characters", MAX_ID_LENGTH));
        }
        if (event.getPageUrl() != null && event.getPageUrl().length() > MAX_URL_LENGTH) {
            errors.add(String.format("pageUrl exceeds maximum length of %d characters", MAX_URL_LENGTH));
        }
        if (event.getReferrerUrl() != null && event.getReferrerUrl().length() > MAX_URL_LENGTH) {
            errors.add(String.format("referrerUrl exceeds maximum length of %d characters", MAX_URL_LENGTH));
        }
        if (event.getTargetElement() != null && event.getTargetElement().length() > MAX_ELEMENT_SELECTOR_LENGTH) {
            errors.add(String.format("targetElement exceeds maximum length of %d characters", MAX_ELEMENT_SELECTOR_LENGTH));
        }
        if (event.getUserAgent() != null && event.getUserAgent().length() > MAX_USER_AGENT_LENGTH) {
            errors.add(String.format("userAgent exceeds maximum length of %d characters", MAX_USER_AGENT_LENGTH));
        }
        if (event.getMetadata() != null && event.getMetadata().getElementText() != null 
                && event.getMetadata().getElementText().length() > MAX_ELEMENT_TEXT_LENGTH) {
            errors.add(String.format("elementText exceeds maximum length of %d characters", MAX_ELEMENT_TEXT_LENGTH));
        }
    }

    private void validateTimestamp(long timestamp, List<String> errors) {
        long now = System.currentTimeMillis();
        
        if (timestamp <= 0) {
            errors.add("timestamp must be positive");
        } else if (timestamp > now + maxFutureDriftMs) {
            errors.add(String.format("timestamp cannot be more than %d ms in the future", maxFutureDriftMs));
        } else if (timestamp < now - maxEventAgeMs) {
            errors.add(String.format("timestamp is too old (>%d ms) - events must be recent", maxEventAgeMs));
        }
    }

    private void validateURL(String url, String fieldName, List<String> errors) {
        try {
            URI uri = URI.create(url);
            String scheme = uri.getScheme();
            if (scheme == null) {
                errors.add(String.format("%s must be a valid absolute URL with http or https scheme", fieldName));
            } else if (!scheme.equals("http") && !scheme.equals("https")) {
                errors.add(String.format("%s must be a valid HTTP(S) URL", fieldName));
            }
        } catch (IllegalArgumentException e) {
            errors.add(String.format("%s is not a valid URL: %s", fieldName, e.getMessage()));
        }
    }

    private void validateNoXSS(ClickEvent event, List<String> errors) {
        // Check string fields for common XSS patterns
        if (event.getTargetElement() != null && XSS_PATTERN.matcher(event.getTargetElement()).find()) {
            errors.add("targetElement contains potential XSS payload - sanitize before use");
        }
        if (event.getPageUrl() != null && XSS_PATTERN.matcher(event.getPageUrl()).find()) {
            errors.add("pageUrl contains potential XSS payload - sanitize before use");
        }
        if (event.getMetadata() != null && event.getMetadata().getElementText() != null 
                && XSS_PATTERN.matcher(event.getMetadata().getElementText()).find()) {
            errors.add("elementText contains potential XSS payload - sanitize before use");
        }
    }

    /**
     * Validates that no PII (Personally Identifiable Information) is present.
     * 
     * <p>Security requirement: sessionId must be opaque (UUID), not derivable from userId.
     */
    private void validateNoPII(ClickEvent event, List<String> errors) {
        // Check that sessionId is not the same as userId (basic sanity)
        if (event.getSessionId() != null && event.getSessionId().equals(event.getUserId())) {
            errors.add("sessionId must not equal userId (security violation)");
        }

        // Check for common PII patterns in metadata elementText (improved regex)
        if (event.getMetadata() != null && event.getMetadata().getElementText() != null) {
            if (PII_PATTERN.matcher(event.getMetadata().getElementText()).find()) {
                errors.add("elementText contains potential PII keywords - sanitize before publishing");
            }
        }

        // User agent should NOT contain raw IP addresses
        if (event.getUserAgent() != null && IP_ADDRESS_PATTERN.matcher(event.getUserAgent()).find()) {
            errors.add("userAgent should not contain IP addresses");
        }
    }

    /**
     * Validates that event metadata is consistent with event type.
     */
    private void validateEventTypeConsistency(ClickEvent event, List<String> errors) {
        EventType type = event.getEventType();

        switch (type) {
            case CLICK:
                if (event.getTargetElement() == null || event.getTargetElement().isBlank()) {
                    errors.add("CLICK events must have targetElement");
                }
                if (event.getMetadata() != null) {
                    if (event.getMetadata().getX() == null || event.getMetadata().getY() == null) {
                        errors.add("CLICK events should have x,y coordinates in metadata");
                    }
                }
                break;

            case SCROLL:
                if (event.getMetadata() == null || event.getMetadata().getScrollDepth() == null) {
                    errors.add("SCROLL events must have scrollDepth in metadata");
                } else {
                    double depth = event.getMetadata().getScrollDepth();
                    if (depth < 0.0 || depth > 1.0) {
                        errors.add("scrollDepth must be between 0.0 and 1.0");
                    }
                }
                break;

            case HOVER:
                if (event.getTargetElement() == null || event.getTargetElement().isBlank()) {
                    errors.add("HOVER events must have targetElement");
                }
                if (event.getMetadata() == null || event.getMetadata().getDurationMs() == null) {
                    errors.add("HOVER events must have durationMs in metadata");
                }
                break;

            case PAGE_VIEW:
                // PAGE_VIEW events have minimal requirements
                // pageUrl is already validated above
                break;

            default:
                errors.add("Unknown event type: " + type);
        }
    }

    /**
     * Checks if the event is valid (has no validation errors).
     * 
     * @param event the event to check
     * @return true if valid, false otherwise
     */
    public boolean isValid(ClickEvent event) {
        return validate(event).isEmpty();
    }
}

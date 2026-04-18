package com.clickstream.model;

/**
 * Enumeration of supported clickstream event types.
 * 
 * <p>Each type represents a specific user interaction that is tracked:
 * <ul>
 *   <li>CLICK - User clicks on an interactive element (button, link, etc.)</li>
 *   <li>PAGE_VIEW - Page load or single-page app route change</li>
 *   <li>SCROLL - Scrolling past a depth threshold (25%, 50%, 75%, 100%)</li>
 *   <li>HOVER - Mouse hover on an element for more than 500ms</li>
 * </ul>
 */
public enum EventType {
    /**
     * Mouse click on interactive element.
     * Key metadata: x, y, targetElement, elementText
     */
    CLICK,

    /**
     * Page load or SPA route change.
     * Key metadata: pageUrl, referrerUrl
     */
    PAGE_VIEW,

    /**
     * Scroll past threshold (25/50/75/100%).
     * Key metadata: scrollDepth
     */
    SCROLL,

    /**
     * Hover on element > 500ms.
     * Key metadata: targetElement, durationMs
     */
    HOVER
}

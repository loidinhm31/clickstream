package com.clickstream.util;

/**
 * Utility for anonymizing IP addresses for GDPR/privacy compliance.
 * 
 * IPv4: Masks last octet (192.168.1.123 → 192.168.1.0)
 * IPv6: Masks last 80 bits (only keeps first 48 bits)
 */
public class IpAnonymizer {

    /**
     * Anonymize IP address by masking portions.
     * 
     * @param ipAddress Raw IP address
     * @return Anonymized IP address
     */
    public static String anonymize(String ipAddress) {
        if (ipAddress == null || ipAddress.isBlank()) {
            return "0.0.0.0";
        }

        // IPv6 detection
        if (ipAddress.contains(":")) {
            return anonymizeIPv6(ipAddress);
        }

        // IPv4
        return anonymizeIPv4(ipAddress);
    }

    private static String anonymizeIPv4(String ip) {
        String[] parts = ip.split("\\.");
        if (parts.length != 4) {
            return "0.0.0.0";
        }
        return parts[0] + "." + parts[1] + "." + parts[2] + ".0";
    }

    private static String anonymizeIPv6(String ip) {
        // Keep first 48 bits (first 3 groups), mask rest
        String[] parts = ip.split(":");
        if (parts.length < 3) {
            return "::";
        }
        return parts[0] + ":" + parts[1] + ":" + parts[2] + "::";
    }
}

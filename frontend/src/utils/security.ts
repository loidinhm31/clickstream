// Utility functions for security and data sanitization

/**
 * Sanitize URL for safe display - prevents XSS attacks
 * Validates URL format and sanitizes dangerous protocols
 */
export function sanitizeUrl(url: string): string {
  try {
    const parsed = new URL(url);
    
    // Only allow safe protocols
    const safeProtocols = ['http:', 'https:'];
    if (!safeProtocols.includes(parsed.protocol)) {
      return url.replace(/[<>'"]/g, ''); // Strip dangerous chars
    }
    
    // Return clean URL without script injection
    return url
      .replace(/<script[^>]*>.*?<\/script>/gi, '')
      .replace(/javascript:/gi, '')
      .replace(/on\w+\s*=/gi, '');
  } catch {
    // Invalid URL - sanitize as text
    return url.replace(/[<>'"]/g, '');
  }
}

/**
 * Validate JWT token format (basic check)
 */
export function isValidJWT(token: string): boolean {
  if (!token || typeof token !== 'string') return false;
  
  const parts = token.split('.');
  if (parts.length !== 3) return false;
  
  // Check if each part is valid base64
  try {
    parts.forEach(part => atob(part.replace(/-/g, '+').replace(/_/g, '/')));
    return true;
  } catch {
    return false;
  }
}

/**
 * Check if JWT token is expired
 */
export function isTokenExpired(token: string): boolean {
  try {
    const parts = token.split('.');
    if (parts.length !== 3) return true;
    
    const payload = JSON.parse(atob(parts[1].replace(/-/g, '+').replace(/_/g, '/')));
    const exp = payload.exp;
    
    if (!exp) return true;
    
    return Date.now() >= exp * 1000;
  } catch {
    return true;
  }
}

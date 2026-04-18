import { describe, it, expect } from 'vitest';
import { decodeArrowMetrics } from '../arrowDecoder';

describe('arrowDecoder', () => {
  it('should return null for invalid buffer', () => {
    const invalidBuffer = new ArrayBuffer(10);
    const result = decodeArrowMetrics(invalidBuffer);
    expect(result).toBeNull();
  });

  it('should handle empty buffer gracefully', () => {
    const emptyBuffer = new ArrayBuffer(0);
    const result = decodeArrowMetrics(emptyBuffer);
    expect(result).toBeNull();
  });

  // Full integration test would require actual Apache Arrow IPC data
  // which would be tested in E2E tests with the real backend
});

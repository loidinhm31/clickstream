import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { MetricValue } from '../MetricValue';

describe('MetricValue', () => {
  it('should render label and value', () => {
    render(<MetricValue label="Active Users" value={100} />);
    expect(screen.getByText('Active Users')).toBeTruthy();
    expect(screen.getByText('100')).toBeTruthy();
  });

  it('should render unit when provided', () => {
    render(<MetricValue label="Speed" value={50} unit="/s" />);
    expect(screen.getByText('/s')).toBeTruthy();
  });

  it('should handle string values', () => {
    render(<MetricValue label="Status" value="Online" />);
    expect(screen.getByText('Online')).toBeTruthy();
  });
});

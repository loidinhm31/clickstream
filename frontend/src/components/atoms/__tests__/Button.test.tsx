import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { Button } from '../Button';

describe('Button', () => {
  it('should render button with text', () => {
    render(<Button>Click me</Button>);
    expect(screen.getByText('Click me')).toBeTruthy();
  });

  it('should apply variant class', () => {
    const { container } = render(<Button variant="primary">Test</Button>);
    const button = container.querySelector('button');
    expect(button?.className).toContain('btn-primary');
  });

  it('should apply size class', () => {
    const { container } = render(<Button size="lg">Test</Button>);
    const button = container.querySelector('button');
    expect(button?.className).toContain('btn-lg');
  });

  it('should respect disabled state', () => {
    const { container } = render(<Button disabled>Test</Button>);
    const button = container.querySelector('button');
    expect(button?.disabled).toBe(true);
  });
});

import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import { Badge, type BadgeVariant } from './Badge';

describe('Badge', () => {
  it('renders every variant', () => {
    const variants: BadgeVariant[] = ['scored', 'running', 'failed', 'disqualified', 'ready', 'requested', 'completed', 'building', 'queued'];
    variants.forEach((variant) => {
      render(<Badge variant={variant} />);
      expect(screen.getByText(variant.toUpperCase())).toBeInTheDocument();
    });
  });
});

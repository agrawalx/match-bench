import { describe, expect, it } from 'vitest';
import { generateChallenge, generateState, generateVerifier } from './pkce';

describe('pkce', () => {
  it('generates a URL-safe verifier within the RFC length bounds', async () => {
    const verifier = await generateVerifier();
    expect(verifier.length).toBeGreaterThanOrEqual(43);
    expect(verifier.length).toBeLessThanOrEqual(128);
    expect(verifier).toMatch(/^[A-Za-z0-9_-]+$/);
  });

  it('generates a URL-safe challenge', async () => {
    const challenge = await generateChallenge('verifier');
    expect(challenge).toMatch(/^[A-Za-z0-9_-]+$/);
    expect(challenge).not.toContain('=');
  });

  it('generates distinct states', () => {
    expect(generateState()).not.toEqual(generateState());
  });
});

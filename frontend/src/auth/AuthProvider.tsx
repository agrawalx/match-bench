'use client';

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
} from 'react';
import { googleClientId, googleRedirectUri, platformConfig } from '@/config/platform';
import { generateChallenge, generateNonce, generateState, generateVerifier } from './pkce';

export type AuthStatus =
  | 'unauthenticated'
  | 'redirecting'
  | 'exchanging'
  | 'authenticated'
  | 'error';

export interface UserProfile {
  sub: string;
  email: string;
  emailVerified: boolean;
  displayName: string;
  avatarUrl: string | null;
  contestantId: string;
}

interface AuthState {
  status: AuthStatus;
  user: UserProfile | null;
  platformToken: string | null;
  platformTokenExpiresAt: number;
  error: string | null;
}

interface AuthContextValue extends AuthState {
  signIn: () => Promise<void>;
  signOut: () => Promise<void>;
  handleCallback: (code: string, returnedState: string) => Promise<void>;
  getToken: () => string | null;
}

const KEYS = {
  verifier: 'iicpc_pkce_verifier',
  state: 'iicpc_pkce_state',
  nonce: 'iicpc_pkce_nonce',
  returnTo: 'iicpc_return_to',
} as const;

const AuthContext = createContext<AuthContextValue | null>(null);

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const [state, setState] = useState<AuthState>({
    status: 'unauthenticated',
    user: null,
    platformToken: null,
    platformTokenExpiresAt: 0,
    error: null,
  });
  const stateRef = useRef(state);
  const refreshTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const refreshRef = useRef<() => Promise<void>>(async () => undefined);

  useEffect(() => {
    stateRef.current = state;
  }, [state]);

  const clearRefreshTimer = useCallback(() => {
    if (refreshTimerRef.current) {
      clearTimeout(refreshTimerRef.current);
      refreshTimerRef.current = null;
    }
  }, []);

  const scheduleRefresh = useCallback(
    (expiresAt: number) => {
      clearRefreshTimer();
      const delay = expiresAt - Date.now() - 60_000;
      refreshTimerRef.current = setTimeout(
        () => void refreshRef.current(),
        Math.max(delay, 0),
      );
    },
    [clearRefreshTimer],
  );

  const performSilentRefresh = useCallback(async () => {
    try {
      const response = await fetch(platformConfig.endpoints.auth.refresh, {
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ client_id: googleClientId() }),
      });

      if (!response.ok) {
        clearRefreshTimer();
        setState({
          status: 'unauthenticated',
          user: null,
          platformToken: null,
          platformTokenExpiresAt: 0,
          error: null,
        });
        return;
      }

      const body = (await response.json()) as TokenExchangeResponse;
      const expiresAt = Date.now() + body.expires_in * 1000;
      setState((current) => ({
        ...current,
        status: 'authenticated',
        user: body.user ?? current.user,
        platformToken: body.platform_token ?? body.id_token ?? body.access_token ?? null,
        platformTokenExpiresAt: expiresAt,
        error: null,
      }));
      scheduleRefresh(expiresAt);
    } catch {
      refreshTimerRef.current = setTimeout(() => void performSilentRefresh(), 30_000);
    }
  }, [clearRefreshTimer, scheduleRefresh]);

  useEffect(() => {
    refreshRef.current = performSilentRefresh;
  }, [performSilentRefresh]);

  useEffect(() => {
    void performSilentRefresh();
  }, [performSilentRefresh]);

  useEffect(() => clearRefreshTimer, [clearRefreshTimer]);

  const signIn = useCallback(async () => {
    if (!googleClientId()) {
      setState((current) => ({
        ...current,
        status: 'error',
        error: 'Google OAuth client ID is not configured.',
      }));
      return;
    }
    setState((current) => ({ ...current, status: 'redirecting', error: null }));
    const verifier = await generateVerifier();
    const challenge = await generateChallenge(verifier);
    const oauthState = generateState();
    const nonce = generateNonce();

    sessionStorage.setItem(KEYS.verifier, verifier);
    sessionStorage.setItem(KEYS.state, oauthState);
    sessionStorage.setItem(KEYS.nonce, nonce);
    sessionStorage.setItem(KEYS.returnTo, window.location.pathname);

    const params = new URLSearchParams({
      client_id: googleClientId(),
      redirect_uri: googleRedirectUri(),
      response_type: 'code',
      scope: 'openid profile email',
      code_challenge: challenge,
      code_challenge_method: 'S256',
      state: oauthState,
      nonce,
      // 'consent' (not 'select_account') so Google returns a refresh_token on
      // EVERY login. With access_type=offline alone, Google only returns a
      // refresh_token on the first-ever authorization; re-logins then yield no
      // refresh_token, the backend sets no refresh cookie, and the session
      // can't survive a page reload (silent /refresh has nothing to use).
      prompt: 'consent',
      access_type: 'offline',
    });

    window.location.assign(`https://accounts.google.com/o/oauth2/v2/auth?${params.toString()}`);
  }, []);

  const handleCallback = useCallback(
    async (code: string, returnedState: string) => {
      setState((current) => ({ ...current, status: 'exchanging', error: null }));
      const savedState = sessionStorage.getItem(KEYS.state);
      const verifier = sessionStorage.getItem(KEYS.verifier);
      const nonce = sessionStorage.getItem(KEYS.nonce);

      if (!savedState || savedState !== returnedState || !verifier || !nonce) {
        clearPkceStorage();
        setState((current) => ({
          ...current,
          status: 'error',
          error: 'OAuth state could not be verified. Please sign in again.',
        }));
        throw new Error('state_mismatch');
      }

      try {
        const response = await fetch(platformConfig.endpoints.auth.token, {
          method: 'POST',
          credentials: 'include',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            code,
            code_verifier: verifier,
            client_id: googleClientId(),
            redirect_uri: googleRedirectUri(),
            nonce,
          }),
        });

        if (!response.ok) {
          const body = (await response.json().catch(() => ({ error: 'exchange_failed' }))) as {
            error?: string;
            message?: string;
          };
          throw new Error(body.error ?? body.message ?? 'exchange_failed');
        }

        const body = (await response.json()) as TokenExchangeResponse;
        const token = body.platform_token ?? body.id_token ?? body.access_token ?? null;
        const user = body.user ?? decodeUserFromIdToken(body.id_token);
        const expiresAt = Date.now() + body.expires_in * 1000;

        setState({
          status: 'authenticated',
          user,
          platformToken: token,
          platformTokenExpiresAt: expiresAt,
          error: null,
        });
        scheduleRefresh(expiresAt);
      } finally {
        clearPkceStorage();
      }
    },
    [scheduleRefresh],
  );

  const signOut = useCallback(async () => {
    clearRefreshTimer();
    await fetch(platformConfig.endpoints.auth.logout, { method: 'POST', credentials: 'include' }).catch(() => undefined);
    setState({
      status: 'unauthenticated',
      user: null,
      platformToken: null,
      platformTokenExpiresAt: 0,
      error: null,
    });
  }, [clearRefreshTimer]);

  const getToken = useCallback(() => stateRef.current.platformToken, []);

  const value = useMemo<AuthContextValue>(
    () => ({ ...state, signIn, signOut, handleCallback, getToken }),
    [state, signIn, signOut, handleCallback, getToken],
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth(): AuthContextValue {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error('useAuth must be used within AuthProvider');
  }
  return context;
}

function clearPkceStorage(): void {
  sessionStorage.removeItem(KEYS.verifier);
  sessionStorage.removeItem(KEYS.state);
  sessionStorage.removeItem(KEYS.nonce);
}

function decodeUserFromIdToken(idToken: string | undefined): UserProfile {
  if (!idToken) {
    return {
      sub: '',
      email: '',
      emailVerified: false,
      displayName: 'Contestant',
      avatarUrl: null,
      contestantId: '',
    };
  }

  const [, payload] = idToken.split('.');
  const decoded = JSON.parse(atob(payload.replace(/-/g, '+').replace(/_/g, '/'))) as {
    sub?: string;
    email?: string;
    email_verified?: boolean;
    name?: string;
    picture?: string;
  };

  return {
    sub: decoded.sub ?? '',
    email: decoded.email ?? '',
    emailVerified: decoded.email_verified ?? false,
    displayName: decoded.name ?? decoded.email ?? 'Contestant',
    avatarUrl: decoded.picture ?? null,
    contestantId: decoded.sub ?? '',
  };
}

interface TokenExchangeResponse {
  platform_token?: string;
  access_token?: string;
  id_token?: string;
  expires_in: number;
  user?: UserProfile;
}

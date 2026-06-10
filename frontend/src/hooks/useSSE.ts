'use client';

import { useEffect, useRef } from 'react';
import type { SSEEvent } from '@/types/leaderboard';

export type SSEStatus = 'connecting' | 'open' | 'closed' | 'error';

interface SSEOptions {
  enabled?: boolean;
  onMessage: (event: SSEEvent) => void;
  onStatusChange?: (status: SSEStatus) => void;
}

// Connects to the leaderboard-api SSE endpoint. The broker emits named events
// ("snapshot" on connect, "update" per change), so we must register listeners
// per event name — the default onmessage handler never fires for named events.
// The endpoint is unauthenticated; never append tokens to the URL (they would
// land in nginx access logs).
export function useSSE(url: string, { enabled = true, onMessage, onStatusChange }: SSEOptions) {
  const messageRef = useRef(onMessage);
  const statusRef = useRef(onStatusChange);

  useEffect(() => {
    messageRef.current = onMessage;
    statusRef.current = onStatusChange;
  }, [onMessage, onStatusChange]);

  useEffect(() => {
    if (!enabled) {
      statusRef.current?.('closed');
      return;
    }

    let source: EventSource | null = null;
    let reconnect: ReturnType<typeof setTimeout> | null = null;
    let closed = false;
    let attempt = 0;

    const dispatch = (type: SSEEvent['type'], raw: string) => {
      try {
        messageRef.current({ type, data: JSON.parse(raw) } as SSEEvent);
      } catch {
        // Ignore malformed events; the stream stays alive.
      }
    };

    const connect = () => {
      statusRef.current?.('connecting');
      source = new EventSource(url);
      source.onopen = () => {
        attempt = 0;
        statusRef.current?.('open');
      };
      source.addEventListener('snapshot', (event) => dispatch('snapshot', (event as MessageEvent).data));
      source.addEventListener('update', (event) => dispatch('update', (event as MessageEvent).data));
      source.onerror = () => {
        statusRef.current?.('error');
        source?.close();
        source = null;
        if (!closed) {
          const delay = Math.min(1000 * 2 ** attempt, 30_000);
          attempt += 1;
          reconnect = setTimeout(connect, delay);
        }
      };
    };

    connect();

    return () => {
      closed = true;
      statusRef.current?.('closed');
      source?.close();
      if (reconnect) clearTimeout(reconnect);
    };
  }, [url, enabled]);
}

'use client';

import { useEffect, useRef } from 'react';
import type { SSEEvent } from '@/types/leaderboard';

export type SSEStatus = 'connecting' | 'open' | 'closed' | 'error';

interface SSEOptions {
  token?: string | null;
  enabled?: boolean;
  onMessage: (event: SSEEvent) => void;
  onStatusChange?: (status: SSEStatus) => void;
}

export function useSSE(url: string, { token, enabled = true, onMessage, onStatusChange }: SSEOptions) {
  const messageRef = useRef(onMessage);
  const statusRef = useRef(onStatusChange);

  useEffect(() => {
    messageRef.current = onMessage;
    statusRef.current = onStatusChange;
  }, [onMessage, onStatusChange]);

  useEffect(() => {
    let source: EventSource | null = null;
    let reconnect: ReturnType<typeof setTimeout> | null = null;
    let closed = false;
    let attempt = 0;

    const connect = () => {
      statusRef.current?.('connecting');
      const separator = url.includes('?') ? '&' : '?';
      source = new EventSource(token ? `${url}${separator}token=${encodeURIComponent(token)}` : url);
      source.onopen = () => {
        attempt = 0;
        statusRef.current?.('open');
      };
      source.onmessage = (event) => {
        try {
          messageRef.current(JSON.parse(event.data) as SSEEvent);
        } catch {
          // Ignore malformed events; the stream stays alive.
        }
      };
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
  }, [url, token, enabled]);
}

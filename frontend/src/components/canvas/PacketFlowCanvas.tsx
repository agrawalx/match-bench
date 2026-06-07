'use client';

import { useEffect, useRef } from 'react';
import styles from './PacketFlowCanvas.module.css';

interface Packet {
  edge: number;
  progress: number;
  speed: number;
  delay: number;
}

const labels = [
  'submission-api',
  'build-worker',
  'sandbox-orchestrator',
  'bot-fleet-controller',
  'bot-fleet',
  'ebpf-latency',
  'telemetry-ingester',
  'correctness-validator',
  'score-computer',
  'leaderboard-api',
];

export function PacketFlowCanvas(props: React.CanvasHTMLAttributes<HTMLCanvasElement>) {
  const ref = useRef<HTMLCanvasElement | null>(null);

  useEffect(() => {
    if (window.matchMedia('(prefers-reduced-motion: reduce)').matches) return undefined;
    const canvas = ref.current;
    if (!canvas) return undefined;
    const context = canvas.getContext('2d');
    if (!context) return undefined;

    let frame = 0;
    let last = performance.now();
    const packets: Packet[] = Array.from({ length: 20 }, () => ({
      edge: Math.floor(Math.random() * (labels.length - 1)),
      progress: Math.random(),
      speed: 80 + Math.random() * 60,
      delay: Math.random() * 2,
    }));

    const resize = () => {
      const ratio = window.devicePixelRatio || 1;
      canvas.width = Math.floor(window.innerWidth * ratio);
      canvas.height = Math.floor(window.innerHeight * ratio);
      canvas.style.width = `${window.innerWidth}px`;
      canvas.style.height = `${window.innerHeight}px`;
      context.setTransform(ratio, 0, 0, ratio, 0, 0);
    };

    const points = () =>
      labels.map((_, index) => {
        const x = 48 + (window.innerWidth - 96) * (index / (labels.length - 1));
        const y = window.innerHeight * (0.35 + 0.25 * Math.sin(index * 0.9));
        return { x, y };
      });

    const draw = (now: number) => {
      const delta = Math.min((now - last) / 1000, 0.016);
      last = now;
      const p = points();
      context.clearRect(0, 0, window.innerWidth, window.innerHeight);
      context.lineWidth = 0.5;
      context.strokeStyle = '#1a1a1a';
      for (let index = 0; index < p.length - 1; index += 1) {
        context.beginPath();
        context.moveTo(p[index].x, p[index].y);
        context.lineTo(p[index + 1].x, p[index + 1].y);
        context.stroke();
      }
      p.forEach((point) => {
        context.beginPath();
        context.arc(point.x, point.y, 4, 0, Math.PI * 2);
        context.fillStyle = '#1c1c1c';
        context.fill();
        context.strokeStyle = '#333333';
        context.lineWidth = 1;
        context.stroke();
      });
      packets.forEach((packet) => {
        if (packet.delay > 0) {
          packet.delay -= delta;
          return;
        }
        const start = p[packet.edge];
        const end = p[packet.edge + 1];
        const distance = Math.hypot(end.x - start.x, end.y - start.y);
        packet.progress += (packet.speed * delta) / distance;
        if (packet.progress >= 1) {
          packet.edge = 0;
          packet.progress = 0;
          packet.delay = 0.5 + Math.random() * 1.5;
          packet.speed = 80 + Math.random() * 60;
          return;
        }
        context.beginPath();
        context.arc(
          start.x + (end.x - start.x) * packet.progress,
          start.y + (end.y - start.y) * packet.progress,
          2,
          0,
          Math.PI * 2,
        );
        context.fillStyle = '#00e5ff';
        context.fill();
      });
      frame = requestAnimationFrame(draw);
    };

    resize();
    window.addEventListener('resize', resize);
    frame = requestAnimationFrame(draw);
    return () => {
      window.removeEventListener('resize', resize);
      cancelAnimationFrame(frame);
    };
  }, []);

  return <canvas {...props} ref={ref} className={styles.canvas} />;
}

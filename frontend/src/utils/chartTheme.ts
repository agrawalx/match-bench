/**
 * Chart theme — the single source of truth for chart series colors and recharts
 * chrome (axis / grid / tooltip), mirroring the light-theme design tokens in
 * styles/tokens.css. recharts renders stroke/fill as SVG presentation attributes,
 * which do NOT resolve CSS var(), so the series palette is duplicated here as
 * concrete hex. Keep these in sync with the --s-* / --grid / --axis tokens.
 */

/** Fixed semantic series palette — reused on every chart. */
export const series = {
  p50: "#0f9a9a",
  p90: "#1f75c4",
  p99: "#4f46e5",
  tps: "#b06f12",
  err: "#c93b40",
} as const;

/** Chart chrome, matching --grid / --axis / surfaces / text tokens. */
export const chart = {
  grid: "#e7ebf1",
  axis: "#79838f",
  axisTick: { fill: "#79838f", fontSize: 11 },
  tooltip: {
    background: "#ffffff",
    border: "1px solid #dce1e9",
    borderRadius: 7,
    color: "#111823",
    fontFamily: "var(--font-mono)",
    fontSize: 12,
  } as const,
} as const;

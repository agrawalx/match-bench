interface Props {
  data?: number[];
  width?: number;
  height?: number;
}

export function LatencySparkline({ data = [], width = 80, height = 24 }: Props) {
  if (data.length < 2) return null;
  let yMin = Math.min(...data) * 0.9;
  let yMax = Math.max(...data) * 1.1;
  if (yMin === yMax) {
    yMin -= 1;
    yMax += 1;
  }
  const points = data
    .map((value, index) => {
      const x = (index / (data.length - 1)) * width;
      const y = height - ((value - yMin) / (yMax - yMin)) * height;
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(' ');

  return (
    <svg width={width} height={height} viewBox={`0 0 ${width} ${height}`} aria-hidden="true">
      <polyline points={points} stroke="#00e5ff" strokeWidth={1} fill="none" strokeLinecap="round" />
    </svg>
  );
}

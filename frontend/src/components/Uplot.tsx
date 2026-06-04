import { useEffect, useRef } from "react";
import uPlot from "uplot";
import "uplot/dist/uPlot.min.css";

// Thin React wrapper around uPlot (canvas-based, handles dense streaming
// time-series without frame drops). Recreates the chart when data or size
// changes and disposes it on unmount.
export default function Uplot({
  data,
  options,
  height = 220,
}: {
  data: uPlot.AlignedData;
  options: Omit<uPlot.Options, "width" | "height">;
  height?: number;
}) {
  const ref = useRef<HTMLDivElement>(null);
  const plot = useRef<uPlot | null>(null);

  useEffect(() => {
    if (!ref.current) return;
    const width = ref.current.clientWidth || 600;
    const u = new uPlot({ ...options, width, height } as uPlot.Options, data, ref.current);
    plot.current = u;
    const onResize = () => u.setSize({ width: ref.current!.clientWidth || width, height });
    window.addEventListener("resize", onResize);
    return () => {
      window.removeEventListener("resize", onResize);
      u.destroy();
      plot.current = null;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [data, options, height]);

  return <div ref={ref} className="uplot-host" />;
}

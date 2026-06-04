import { useEffect, useMemo, useState } from "react";
import {
  createColumnHelper,
  flexRender,
  getCoreRowModel,
  getSortedRowModel,
  useReactTable,
  type SortingState,
} from "@tanstack/react-table";
import {
  fmtMicros,
  fmtPct,
  fmtTPS,
  getLeaderboard,
  subscribeLeaderboard,
  type LeaderboardRow,
} from "../api";

const col = createColumnHelper<LeaderboardRow>();

function RankDelta({ delta }: { delta: number }) {
  if (delta > 0) return <span className="delta-up" title={`up ${delta}`}>▲{delta}</span>;
  if (delta < 0) return <span className="delta-down" title={`down ${-delta}`}>▼{-delta}</span>;
  return <span className="delta-flat">—</span>;
}

export default function Leaderboard({ onSelect }: { onSelect: (runGroupID: string) => void }) {
  const [rows, setRows] = useState<LeaderboardRow[]>([]);
  const [source, setSource] = useState<string>("");
  const [sorting, setSorting] = useState<SortingState>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let alive = true;
    getLeaderboard()
      .then((r) => {
        if (!alive) return;
        setRows(r.rows);
        setSource(r.source);
        setLoading(false);
      })
      .catch(() => setLoading(false));
    const unsub = subscribeLeaderboard((r) => {
      if (!alive) return;
      setRows(r.rows);
      setSource(r.source);
    });
    return () => {
      alive = false;
      unsub();
    };
  }, []);

  const columns = useMemo(
    () => [
      col.accessor("rank", {
        header: "#",
        cell: (c) => <span className="rank">{c.getValue()}</span>,
      }),
      col.accessor("team_name", { header: "Team" }),
      col.accessor("peak_sustained_tps", {
        header: "Peak sustained TPS",
        cell: (c) => <span className="mono">{fmtTPS(c.getValue())}</span>,
        sortDescFirst: true,
      }),
      col.accessor("p99_ns_at_peak_tps", {
        header: "p99 @ peak",
        cell: (c) => fmtMicros(c.getValue()),
      }),
      col.accessor("total_correctness", {
        header: "Correctness",
        cell: (c) => fmtPct(c.getValue()),
      }),
      col.accessor("rank_delta", {
        header: "Δ",
        cell: (c) => <RankDelta delta={c.getValue()} />,
        enableSorting: false,
      }),
      col.accessor("disqualified", {
        header: "Status",
        cell: (c) =>
          c.getValue() ? (
            <span className="badge dq" title={c.row.original.disqualification_code}>DQ</span>
          ) : (
            <span className="badge ok">OK</span>
          ),
      }),
    ],
    [],
  );

  const table = useReactTable({
    data: rows,
    columns,
    state: { sorting },
    onSortingChange: setSorting,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
  });

  const numericCols = new Set(["peak_sustained_tps", "p99_ns_at_peak_tps", "total_correctness", "rank_delta"]);

  return (
    <div className="card" data-testid="leaderboard">
      <h2>Live leaderboard</h2>
      <div className="hint">
        Click a column to sort, or a row to open the run detail. Updates stream live
        {source ? ` (source: ${source})` : ""}.
      </div>
      {loading ? (
        <div className="empty">Loading…</div>
      ) : rows.length === 0 ? (
        <div className="empty">No scored submissions yet.</div>
      ) : (
        <table>
          <thead>
            {table.getHeaderGroups().map((hg) => (
              <tr key={hg.id}>
                {hg.headers.map((h) => {
                  const numeric = numericCols.has(h.column.id);
                  const dir = h.column.getIsSorted();
                  return (
                    <th
                      key={h.id}
                      className={numeric ? "num" : ""}
                      onClick={h.column.getToggleSortingHandler()}
                      data-testid={`th-${h.column.id}`}
                    >
                      {flexRender(h.column.columnDef.header, h.getContext())}
                      {dir && <span className="arrow">{dir === "asc" ? "↑" : "↓"}</span>}
                    </th>
                  );
                })}
              </tr>
            ))}
          </thead>
          <tbody>
            {table.getRowModel().rows.map((r) => (
              <tr
                key={r.id}
                className={r.original.disqualified ? "dq" : ""}
                onClick={() => onSelect(r.original.run_group_id)}
                data-testid={`row-${r.original.run_group_id}`}
              >
                {r.getVisibleCells().map((cell) => (
                  <td key={cell.id} className={numericCols.has(cell.column.id) ? "num" : ""}>
                    {flexRender(cell.column.columnDef.cell, cell.getContext())}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}

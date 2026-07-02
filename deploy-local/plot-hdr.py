#!/usr/bin/env python3
"""Plot HDR latency for a run group from TimescaleDB.

Data is pulled via `kubectl exec` (no port-forward/password needed). For each
scenario session we take the LAST hdr_encoded blob per wave (the blobs are
cumulative within a wave), decode the Rust V2-deflate HDR histograms with the
Python hdrh implementation (independent cross-check), merge them, and render:
  1) a log-x percentile curve per scenario  -> hdr_percentiles.png
  2) p50/p99 latency over time              -> latency_timeline.png
  3) throughput (tps) over time             -> throughput_timeline.png
Also emits standard .hgrm percentile files for each scenario.
"""
import subprocess, base64, sys, csv, io
from collections import defaultdict
from hdrh.histogram import HdrHistogram
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

RUN_GROUP = sys.argv[1] if len(sys.argv) > 1 else None
OUT = "deploy-local/plots"
import os; os.makedirs(OUT, exist_ok=True)

# session_id -> scenario name (from the metadata DB via kubectl)
def psql(db, sql):
    cmd = ["kubectl","exec","-n","data",
           "timescaledb-0" if db=="metrics" else "postgres-0","-c",
           "timescaledb" if db=="metrics" else "postgres","--",
           "psql","-U","iicpc","-d",db,"-tAF,","-c",sql]
    return subprocess.run(cmd, capture_output=True, text=True).stdout

# map sessions -> scenario for this run group (fall back to all sessions)
sess_scen = {}
if RUN_GROUP:
    rows = psql("iicpc", f"select r.session_id, s.name from runs r join scenarios s on s.scenario_id=r.scenario_id where r.run_group_id='{RUN_GROUP}';")
    for ln in rows.strip().splitlines():
        if "," in ln:
            sid, name = ln.split(",",1); sess_scen[sid]=name
if not sess_scen:
    rows = psql("metrics","select distinct session_id from metrics;")
    for sid in rows.strip().splitlines():
        sess_scen[sid.strip()] = sid.strip()[:8]
print("sessions:", sess_scen)

# ---- 1) HDR percentile curves (last blob per wave, merged per scenario) ----
plt.figure(figsize=(9,5.5))
any_hdr = False
for sid, name in sess_scen.items():
    rows = psql("metrics", f"""
      select wave_index, replace(encode(hdr_encoded,'base64'),chr(10),'')
      from (select distinct on (wave_index) wave_index, time, hdr_encoded
            from metrics where session_id='{sid}' and hdr_encoded is not null
            order by wave_index, time desc) t order by wave_index;""")
    merged = None
    for ln in rows.strip().splitlines():
        if "," not in ln: continue
        _, b64 = ln.split(",",1)
        if not b64.strip(): continue
        h = HdrHistogram.decode(b64.strip())
        if merged is None:
            merged = HdrHistogram(h.lowest_trackable_value, h.highest_trackable_value, h.significant_figures)
        merged.add(h)
    if merged is None or merged.total_count == 0: continue
    any_hdr = True
    # percentile curve: x = 1/(1-p) (the "nines"), y = value in microseconds
    xs, ys = [], []
    for pct in [0,10,25,50,75,90,95,99,99.9,99.99]:
        xs.append(100/(100-pct) if pct<100 else 1e6)
        ys.append(merged.get_value_at_percentile(pct)/1000.0)
    plt.semilogx(xs, ys, marker="o", label=f"{name} (n={merged.total_count}, p99={merged.get_value_at_percentile(99)/1000:.0f}us)")
    # .hgrm export
    with open(f"{OUT}/{name}.hgrm","w") as f:
        f.write("Value(us)  Percentile  TotalCount  1/(1-P)\n")
        for pct in [0,25,50,75,90,99,99.9,99.99,100 if False else 99.99]:
            v=merged.get_value_at_percentile(pct)
            f.write(f"{v/1000:.3f}  {pct/100:.5f}  {merged.get_count_at_value(v)}\n")
if any_hdr:
    plt.xlabel("Percentile  (1/(1-p) — log scale)"); plt.ylabel("service_time t7-t3 (microseconds)")
    plt.title(f"HDR latency percentiles per scenario\nrun_group {RUN_GROUP or '(all)'}")
    plt.grid(True, which="both", alpha=0.3); plt.legend()
    plt.tight_layout(); plt.savefig(f"{OUT}/hdr_percentiles.png", dpi=130); print(f"wrote {OUT}/hdr_percentiles.png")
else:
    print("no HDR data found")

# ---- 2 & 3) timelines from the metric columns ----
for sid, name in sess_scen.items():
    rows = psql("metrics", f"select extract(epoch from time), p50_ns, p99_ns, tps_1s from metrics where session_id='{sid}' order by time;")
    t0=None; T=[]; P50=[]; P99=[]; TPS=[]
    for ln in rows.strip().splitlines():
        p=ln.split(",")
        if len(p)<4 or not p[0]: continue
        ts=float(p[0]); t0=ts if t0 is None else t0
        T.append(ts-t0); P50.append(float(p[1])/1000); P99.append(float(p[2])/1000); TPS.append(float(p[3]))
    if not T: continue
    plt.figure(figsize=(9,4))
    plt.plot(T,P50,label="p50 us"); plt.plot(T,P99,label="p99 us")
    plt.xlabel("seconds into session"); plt.ylabel("latency (us)"); plt.title(f"latency timeline — {name}")
    plt.grid(alpha=0.3); plt.legend(); plt.tight_layout(); plt.savefig(f"{OUT}/latency_{name}.png", dpi=120)
    plt.figure(figsize=(9,3.5))
    plt.plot(T,TPS,color="green"); plt.xlabel("seconds into session"); plt.ylabel("TPS"); plt.title(f"throughput timeline — {name}")
    plt.grid(alpha=0.3); plt.tight_layout(); plt.savefig(f"{OUT}/throughput_{name}.png", dpi=120)
    print(f"wrote {OUT}/latency_{name}.png, {OUT}/throughput_{name}.png")
print("DONE ->", OUT)

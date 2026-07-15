#!/usr/bin/env python3
"""Render the RSS-over-time chart: streaming ramp vs in-memory plateau vs
selective-flat vs DuckDB-flat. All curves measured on the same 3.56 GB file."""
import json
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = os.path.dirname(os.path.abspath(__file__))
res = json.load(open(os.path.join(HERE, "results.json")))
thr = json.load(open(os.path.join(HERE, "threads_results.json")))


def norm(curve, t_shift):
    return [t - t_shift for t, _ in curve], [m for _, m in curve]


series = [
    ("polars streaming — full scan (current)", res["rep"]["full_current"], "#d1495b", "-"),
    ("polars in-memory — full scan",           res["rep"]["full_inmem"],   "#8d99ae", "--"),
    ("polars streaming — selective (0.1%)",    res["rep"]["sel_current"],  "#2a9d8f", "-"),
    ("DuckDB — full scan (same query)",        thr["duckdb_t4"],           "#264653", "-"),
]

fig, ax = plt.subplots(figsize=(11, 6.2))
for label, r, color, ls in series:
    curve = r["curve"]
    t_pre = r.get("t_pre_ms", 0)
    # shift so collect starts at t=0
    t0 = curve[0][0]
    xs = [t - t0 for t, _ in curve]
    ys = [m for _, m in curve]
    ax.plot(xs, ys, label=f"{label}  (peak {r['peak_maxrss_mb']:.0f} MB)", color=color, ls=ls, lw=2)

ax.axhline(3560, color="#999", lw=0.8, ls=":", alpha=0.7)
ax.text(50, 3610, "3.56 GB file size on disk", fontsize=8, color="#666")
ax.set_xlabel("time since collect start (ms)")
ax.set_ylabel("resident memory / RSS (MB)")
ax.set_title("polars-gw group_by(city).agg — RSS over a single collect (100M rows, 3.56 GB, 4 cores)\n"
             "streaming 'works' (no fallback) yet RAMPS to ~1.5 GB; DuckDB & selective stay flat & low",
             fontsize=11)
ax.legend(loc="upper left", fontsize=9, framealpha=0.95)
ax.grid(True, alpha=0.25)
ax.set_ylim(0, 4400)
fig.tight_layout()
out = os.path.join(HERE, "rss_curves.png")
fig.savefig(out, dpi=130)
print("wrote", out)

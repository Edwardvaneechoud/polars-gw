#!/usr/bin/env python3
"""
Render blog-ready charts from benchmark_results.json.

  python visualize.py                       # reads ./benchmark_results.json
  python visualize.py path/to/results.json  # explicit

Writes PNG (150 dpi) + SVG into ./charts/. Palette and contrast were validated
with the data-viz skill's validator (blue/green/magenta/yellow categorical set,
CVD-safe all-pairs); the two low-contrast slots (magenta, yellow) carry a
secondary encoding — distinct line styles + markers + direct value labels — so
identity never rests on color alone.
"""
from __future__ import annotations

import json
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

HERE = os.path.dirname(os.path.abspath(__file__))

# ---- validated palette (light surface) -------------------------------------
SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK2 = "#52514e"
MUTED = "#898781"
GRID = "#e1e0d9"
BASE = "#c3c2b7"
CRIT = "#d03b3b"  # status:critical, reserved — used only for OOM/error marks

# per-path identity: color + linestyle + marker (color is fixed-order categorical;
# linestyle/marker are the secondary encoding for the CVD/contrast relief rule)
STYLE = {
    "polars-gw (LazyFrame)":         dict(color="#2a78d6", ls="-",   marker="o"),
    "polars-gw (DataFrame, eager)":  dict(color="#008300", ls="--",  marker="s"),
    "pygwalker + DuckDB Connector":  dict(color="#e87ba4", ls="-.",  marker="^"),
    "pygwalker (native Polars)":     dict(color="#eda100", ls=":",   marker="D"),
}


def _style():
    plt.rcParams.update({
        "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
        "font.family": "sans-serif",
        "font.sans-serif": ["DejaVu Sans", "Segoe UI", "Helvetica", "Arial"],
        "text.color": INK, "axes.labelcolor": INK2, "axes.edgecolor": BASE,
        "xtick.color": MUTED, "ytick.color": MUTED, "xtick.labelcolor": INK2, "ytick.labelcolor": INK2,
        "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8, "axes.axisbelow": True,
        "axes.spines.top": False, "axes.spines.right": False,
        "axes.titlesize": 13, "axes.titleweight": "bold", "axes.titlecolor": INK,
        "font.size": 10.5, "legend.frameon": False, "figure.dpi": 120,
    })


def _save(fig, name):
    os.makedirs(os.path.join(HERE, "charts"), exist_ok=True)
    for ext in ("png", "svg"):
        fig.savefig(os.path.join(HERE, "charts", f"{name}.{ext}"), bbox_inches="tight", dpi=150)
    plt.close(fig)
    print(f"  charts/{name}.png / .svg")


def _subtitle(meta):
    return (f"{meta['rows']:,} rows · {meta['file_gb']:.2f} GB Parquet · {meta['cores']} cores · "
            f"polars {meta['polars']} · {meta['cache']} cache · median of {meta['trials']}×{meta['reps']}")


def _order(meta):
    return [n for n in meta["names"]]


# ===========================================================================
# 1. RSS over time — the headline memory profile
# ===========================================================================
def fig_rss_curves(d):
    meta, curves = d["meta"], d["curves"]
    fig, ax = plt.subplots(figsize=(10.5, 6.0))
    for name in _order(meta):
        c = curves.get(name, {})
        if c.get("error") or "curve" not in c:
            continue
        st = STYLE[name]
        xs = [t for t, _ in c["curve"]]
        ys = [m for _, m in c["curve"]]
        ax.plot(xs, ys, color=st["color"], ls=st["ls"], lw=2.0,
                marker=st["marker"], markevery=max(1, len(xs) // 12), markersize=5.5,
                markeredgecolor=SURFACE, markeredgewidth=0.6,
                label=f"{name}  (peak {c['peak_mb']:.0f} MB)")
        # mark end of load phase (where eager pays its one-time cost)
        if c.get("t_load_end", 0) > 5:
            ax.axvline(c["t_load_end"], color=st["color"], lw=0.8, ls=":", alpha=0.35)
    if meta["file_gb"] > 0.2:
        ax.axhline(meta["file_gb"] * 1000, color=MUTED, lw=0.8, ls=":", alpha=0.6)
        ax.text(ax.get_xlim()[1], meta["file_gb"] * 1000, f"  file = {meta['file_gb']:.2f} GB",
                fontsize=8, color=MUTED, va="center")
    ax.set_xlabel("time since process start (ms)  —  load, then first full-scan chart")
    ax.set_ylabel("resident memory · RSS (MB)")
    ax.set_title("Memory profile: open the file and render the first full-scan chart")
    ax.set_ylim(bottom=0)
    ax.legend(loc="upper right", fontsize=9)
    fig.text(0.5, -0.02, _subtitle(meta) + "   ·   ONE open→first-full-scan lifecycle (cf. chart 2 = whole-session peak)",
             ha="center", fontsize=8.5, color=MUTED)
    _save(fig, "1_rss_over_time")


# ===========================================================================
# 2. Peak RSS — feasibility
# ===========================================================================
def fig_peak_rss(d):
    meta, R = d["meta"], d["results"]
    names = _order(meta)
    fig, ax = plt.subplots(figsize=(9.5, 4.6))
    ys = list(range(len(names)))[::-1]
    for y, name in zip(ys, names):
        r = R[name]
        st = STYLE[name]
        if r.get("error"):
            ax.barh(y, 0.02, color=CRIT, alpha=0.15, height=0.62)
            ax.text(0.02, y, f"  {r['error']} (address-space cap)", va="center", ha="left",
                    color=CRIT, fontsize=10, fontweight="bold")
            continue
        val = r["peak_rss_gb"]
        ax.barh(y, val, color=st["color"], height=0.62, edgecolor=SURFACE, linewidth=1.2)
        ax.text(val, y, f"  {val:.2f} GB", va="center", ha="left", color=INK2, fontsize=10)
    ax.set_yticks(ys)
    ax.set_yticklabels(names, color=INK, fontsize=10)
    ax.set_xlabel("peak resident memory (GB, lower is better)")
    ax.set_title("Peak memory over the whole session — a path that OOMs is unavailable, not slow")
    ax.grid(axis="y", visible=False)
    ax.set_xlim(0, max([R[n]["peak_rss_gb"] for n in names if not R[n].get("error")], default=1) * 1.25)
    fig.text(0.5, -0.04, _subtitle(meta) + "   ·   ru_maxrss across all 3 query shapes × reps",
             ha="center", fontsize=8.5, color=MUTED)
    _save(fig, "2_peak_rss")


# ===========================================================================
# 3. Per-interaction latency — small multiples, one panel per query
# ===========================================================================
def fig_latency(d):
    meta, R = d["meta"], d["results"]
    names = _order(meta)
    qs = meta["queries"]
    fig, axes = plt.subplots(1, len(qs), figsize=(4.4 * len(qs), 4.8), sharey=False)
    if len(qs) == 1:
        axes = [axes]
    for ax, q in zip(axes, qs):
        ys = list(range(len(names)))[::-1]
        for y, name in zip(ys, names):
            r = R[name]
            st = STYLE[name]
            if r.get("error"):
                ax.text(0.02, y, f"{r['error']}", va="center", color=CRIT, fontsize=9, fontweight="bold")
                continue
            med, lo, hi = r["steady"][q]
            ax.barh(y, med, color=st["color"], height=0.6, edgecolor=SURFACE, linewidth=1.2)
            ax.plot([lo, hi], [y, y], color=INK2, lw=1.3, alpha=0.8)  # min-max whisker
            ax.text(hi, y, f"  {med:.1f}", va="center", ha="left", color=INK2, fontsize=9.5)
        ax.set_yticks(ys)
        ax.set_yticklabels([n.replace(" (", "\n(") for n in names] if ax is axes[0] else [],
                           fontsize=8.5, color=INK)
        ax.set_title(q, fontsize=11.5)
        ax.set_xlabel("ms / interaction")
        ax.grid(axis="y", visible=False)
        ax.set_xlim(0, max(R[n]["steady"][q][2] for n in names if not R[n].get("error")) * 1.22)
    fig.suptitle("Per-interaction latency — steady state, one Graphic Walker query (whisker = min–max)",
                 fontsize=13, fontweight="bold", color=INK, y=1.02)
    fig.text(0.5, -0.02, _subtitle(meta) + "   ·   overlapping whiskers ⇒ no real difference",
             ha="center", fontsize=8.5, color=MUTED)
    fig.tight_layout()
    _save(fig, "3_per_interaction_latency")


# ===========================================================================
# 4. Time to first chart — one_time + first[headline]
# ===========================================================================
def fig_first_chart(d):
    meta, R = d["meta"], d["results"]
    names = _order(meta)
    hq = meta["headline"]
    fig, ax = plt.subplots(figsize=(9.5, 4.6))
    ys = list(range(len(names)))[::-1]
    for y, name in zip(ys, names):
        r = R[name]
        st = STYLE[name]
        if r.get("error"):
            ax.text(0.02, y, f"  {r['error']}", va="center", color=CRIT, fontsize=10, fontweight="bold")
            continue
        load = r["one_time_s"] * 1000
        first = r["first"][hq][0]
        ax.barh(y, load, color=st["color"], height=0.6, edgecolor=SURFACE, linewidth=1.2, alpha=0.55)
        ax.barh(y, first, left=load, color=st["color"], height=0.6, edgecolor=SURFACE, linewidth=1.2)
        ax.text(load + first, y, f"  {(load+first)/1000:.2f}s", va="center", ha="left", color=INK2, fontsize=10)
    ax.set_yticks(ys)
    ax.set_yticklabels(names, color=INK, fontsize=10)
    ax.set_xlabel("milliseconds  ·  faded = one-time load/connect, solid = first query")
    ax.set_title(f"Time to first chart ({hq}) — load + the first query the user waits through")
    ax.grid(axis="y", visible=False)
    fig.text(0.5, -0.04, _subtitle(meta), ha="center", fontsize=8.5, color=MUTED)
    _save(fig, "4_time_to_first_chart")


# ===========================================================================
# 5. Session amortization — session(n) = one_time + n · per_interaction
# ===========================================================================
def fig_amortization(d):
    meta, R = d["meta"], d["results"]
    names = _order(meta)
    qs = meta["queries"]
    mix = meta["mix"]
    # blended per-interaction cost: mix selective, rest full scan (if both exist)
    weights = {q: 0.0 for q in qs}
    if "selective" in qs and "full scan" in qs:
        weights["selective"] = mix
        weights["full scan"] = 1 - mix
        blend_label = f"{mix:.0%} selective / {1-mix:.0%} full scan"
    else:
        for q in qs:
            weights[q] = 1 / len(qs)
        blend_label = "even mix"

    ns = list(range(1, 1001))
    fig, ax = plt.subplots(figsize=(10.5, 5.8))
    for name in names:
        r = R[name]
        st = STYLE[name]
        if r.get("error"):
            continue
        per = sum(w * r["steady"][q][0] for q, w in weights.items()) / 1000  # s
        base = r["one_time_s"]
        ys = [base + n * per for n in ns]
        ax.plot(ns, ys, color=st["color"], ls=st["ls"], lw=2.0,
                marker=st["marker"], markevery=[0, 9, 49, 199, 999], markersize=6,
                markeredgecolor=SURFACE, markeredgewidth=0.7,
                label=f"{name}  ({per*1000:.1f} ms/query, {base:.2f}s load)")
    ax.set_xscale("log")
    ax.set_xlabel("queries in the session  (n — every field drag, filter tweak, re-spec)")
    ax.set_ylabel("cumulative session time (s)")
    ax.set_title(f"Session cost  =  one-time load  +  n × per-interaction   ({blend_label})")
    ax.legend(loc="upper left", fontsize=9)
    ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{int(v)}"))
    for n_anchor in (1, 10, 50, 200, 1000):
        ax.axvline(n_anchor, color=GRID, lw=0.8, alpha=0.7)
    fig.text(0.5, -0.02, _subtitle(meta) + "   ·   warm cache flatters lazy paths on full scans — run --cold too",
             ha="center", fontsize=8.5, color=MUTED)
    _save(fig, "5_session_amortization")


def render(json_path=None):
    json_path = json_path or os.path.join(HERE, "benchmark_results.json")
    with open(json_path) as f:
        d = json.load(f)
    _style()
    print("rendering charts:")
    fig_rss_curves(d)
    fig_peak_rss(d)
    fig_latency(d)
    fig_first_chart(d)
    fig_amortization(d)
    print(f"done -> {os.path.join(HERE, 'charts')}/")


if __name__ == "__main__":
    render(sys.argv[1] if len(sys.argv) > 1 else None)

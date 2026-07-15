#!/usr/bin/env python3
"""Analyze RSS curves from results.json: shape (ramp vs plateau), ASCII sparkline."""
import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))
res = json.load(open(os.path.join(HERE, "results.json")))
rep = res["rep"]

BLOCKS = " ▁▂▃▄▅▆▇█"


def spark(curve, t_pre, t_post, width=70):
    if not curve:
        return "(no samples)"
    tmax = curve[-1][0]
    lo = min(m for _, m in curve)
    hi = max(m for _, m in curve)
    rng = hi - lo or 1
    cells = []
    for i in range(width):
        t_a = tmax * i / width
        t_b = tmax * (i + 1) / width
        vals = [m for t, m in curve if t_a <= t < t_b]
        if not vals:
            cells.append(" ")
            continue
        v = max(vals)
        cells.append(BLOCKS[min(8, int((v - lo) / rng * 8))])
    return "".join(cells)


for name in ["full_current", "full_no_order", "bare_true", "bare_false", "full_inmem", "sel_current"]:
    r = rep[name]
    if "error" in r:
        continue
    curve = r["curve"]
    t_pre, t_post = r["t_pre_ms"], r["t_post_ms"]
    during = [(t, m) for t, m in curve if t_pre <= t <= t_post]
    lo = min((m for _, m in during), default=0)
    hi = max((m for _, m in during), default=0)
    # ramp metric: RSS at 25% vs 75% of collect window
    if during:
        span = t_post - t_pre
        q1 = [m for t, m in during if t <= t_pre + 0.25 * span]
        q3 = [m for t, m in during if t >= t_pre + 0.75 * span]
        r25 = max(q1) if q1 else lo
        r75 = max(q3) if q3 else hi
    else:
        r25 = r75 = 0
    shape = "RAMP" if (r75 - r25) > 0.4 * (hi - r["baseline_mb"]) else "PLATEAU"
    print(f"\n{name}  engine={r['engine']}  collect={r['collect_ms']:.0f}ms  "
          f"baseline={r['baseline_mb']:.0f}MB  peak={hi:.0f}MB  shape={shape}")
    print(f"   during-collect RSS: min={lo:.0f}  q25={r25:.0f}  q75={r75:.0f}  max={hi:.0f} MB")
    print(f"   {spark(curve, t_pre, t_post)}")
    print(f"   ^collect starts ~{t_pre:.0f}ms, ends ~{t_post:.0f}ms, total window {t_post:.0f}ms + 300ms tail")

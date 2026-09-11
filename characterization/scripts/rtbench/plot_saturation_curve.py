#!/usr/bin/env python3
"""Combines summary.json from multiple rtbench result directories (e.g. a
coarse sweep plus a denser follow-up sweep over a narrower load range) into
one utilization/queue-growth-vs-offered-load curve -- for pinning down the
saturation boundary precisely across separate runs instead of re-plotting
each run's own narrow load range in isolation.

Usage:
    python characterization/scripts/rtbench/plot_saturation_curve.py \\
        --results-dir characterization/results_rtbench_pi \\
        --results-dir characterization/results_rtbench_boundary \\
        --out characterization/results_rtbench_boundary/plots/saturation_curve.png
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


def _collect(results_dirs: list[Path]) -> list[dict]:
    by_load: dict[float, dict] = {}
    for d in results_dirs:
        summary = json.loads((d / "summary.json").read_text())
        for lv in summary.get("exp2", []) + summary.get("exp3", []):
            by_load[lv["offered_load"]] = lv  # later dirs win on overlap
    return [by_load[k] for k in sorted(by_load)]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--results-dir", type=Path, action="append", required=True, dest="results_dirs")
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()

    levels = _collect(args.results_dirs)
    loads = [lv["offered_load"] for lv in levels]
    # rho = service_time_s / arrival_interval_s = service_time_s * offered_rate_per_s
    # (offered_rate_per_s == 1 / arrival_interval_s by construction).
    rhos = [(lv["service_time_ms"]["mean"] / 1000.0) * lv["offered_rate_per_s"] for lv in levels]
    growth = [lv["queue_growth_rate_per_s"] for lv in levels]
    colors = {"SUSTAINABLE": "tab:green", "BORDERLINE": "tab:orange", "UNSUSTAINABLE": "tab:red"}

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(9, 8), sharex=True)

    ax1.plot(loads, rhos, color="gray", linewidth=1, zorder=1)
    for lv, rho in zip(levels, rhos):
        ax1.scatter(lv["offered_load"], rho, color=colors.get(lv["verdict"], "gray"), s=60, zorder=2)
    ax1.axhline(1.0, color="k", linestyle="--", alpha=0.5, label="rho=1 (theoretical breakeven)")
    ax1.set_ylabel("Utilization rho\n(service_time / arrival_interval)")
    ax1.set_title("Saturation boundary: utilization and queue growth vs offered load")
    ax1.legend()

    ax2.plot(loads, growth, color="gray", linewidth=1, zorder=1)
    for lv, g in zip(levels, growth):
        ax2.scatter(lv["offered_load"], g, color=colors.get(lv["verdict"], "gray"), s=60, zorder=2)
    ax2.axhline(0.0, color="k", linestyle="--", alpha=0.5)
    ax2.set_xlabel("Offered load (x realtime)")
    ax2.set_ylabel("Queue growth rate\n(requests/s)")

    handles = [plt.Line2D([0], [0], marker="o", color="w", markerfacecolor=c, markersize=9, label=v)
               for v, c in colors.items()]
    ax2.legend(handles=handles)

    fig.tight_layout()
    args.out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(args.out, dpi=140)
    print(f"Wrote {args.out}")

    print("\nCombined data points:")
    for lv, rho, g in zip(levels, rhos, growth):
        print(f"  {lv['offered_load']:.2f}x  rho={rho:.3f}  growth={g:+.4f} req/s  verdict={lv['verdict']}")


if __name__ == "__main__":
    main()

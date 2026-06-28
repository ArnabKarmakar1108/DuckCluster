#!/usr/bin/env python3
"""Generate README-friendly benchmark PNGs from docs/BENCHMARK.md canonical numbers."""

from __future__ import annotations

import math
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.colors as mcolors
import numpy as np

ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = ROOT / "docs" / "plots"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Palette — cohesive, accessible on light GitHub README background
DC = "#1d4ed8"
DC_LIGHT = "#93c5fd"
SN = "#b91c1c"
SN_LIGHT = "#fca5a5"
WIN = "#059669"
WIN_LIGHT = "#6ee7b7"
LOSE = "#9ca3af"
INK = "#111827"
MUTED = "#6b7280"
PANEL = "#f8fafc"
GRID = "#e2e8f0"

SCALE_LABELS = ["SF0.01", "SF1", "SF10", "SF20", "SF40"]
SUITE_RATIOS = [66.4, 7.2, 4.7, 3.8, 2.5]
DC_WINS = [0, 2, 1, 2, 7]
NEAR_PARITY = [0, 0, 2, 3, 2]

CROSSOVER_QUERIES = {
    "Q01": [20.2, 7.5, 1.3, 0.69, 0.40],
    "Q04": [31.0, 14.1, 3.2, 0.74, 0.40],
    "Q06": [77.5, 15.6, 1.2, 1.1, 0.61],
    "Q11": [71.2, 1.4, 2.9, 1.3, 0.68],
    "Q22": [10.0, 0.8, 5.0, 1.03, 0.63],
    "Q02": [5.5, 1.7, 0.5, 1.1, 0.93],
    "Q20": [5.6, 0.9, 9.0, 1.2, 0.87],
    "Q19": [26.7, 4.5, 1.7, 1.3, 1.07],
    "Q21": [11.0, 3.2, 2.3, 1.9, 1.11],
    # Join/merge-heavy outliers (canonical ratios from BENCHMARK.md)
    "Q03": [31.6, 49.1, 2.7, 4.3, 3.1],
    "Q07": [26.9, 27.9, 2.1, 4.5, 3.4],
    "Q08": [22.9, 4.2, 3.6, 6.5, 4.1],
    "Q09": [62.6, 3.8, 2.0, 5.6, 3.9],
    "Q18": [22.7, 5.6, 2.4, 7.3, 5.0],
}

HEATMAP_QUERIES = [
    "Q01", "Q04", "Q06", "Q11", "Q22", "Q02", "Q20", "Q19", "Q21",
    "Q03", "Q07", "Q08", "Q09", "Q18",
]

SF40 = {
    "queries": [
        "Q01", "Q02", "Q03", "Q04", "Q05", "Q06", "Q07", "Q08", "Q09", "Q10",
        "Q11", "Q12", "Q13", "Q14", "Q15", "Q16", "Q17", "Q18", "Q19", "Q20",
        "Q21", "Q22",
    ],
    "dc_ms": [
        3184, 851, 21847, 2178, 15813, 741, 20158, 29308, 67742, 21149,
        519, 8394, 39362, 3374, 5108, 11278, 28056, 81819, 6745, 4102,
        21537, 2451,
    ],
    "sn_ms": [
        8041, 912, 6972, 5386, 7114, 1213, 5963, 7201, 17586, 8086,
        762, 3729, 16678, 2451, 2003, 2993, 8087, 16379, 6309, 4731,
        19488, 3877,
    ],
}

DC_SUITE_S = [130.9, 252, 396]
SN_SUITE_S = [27.9, 66, 156]
SCALE_LARGE = ["SF10", "SF20", "SF40"]

STYLE = {
    "font.family": "sans-serif",
    "font.sans-serif": ["DejaVu Sans", "Helvetica", "Arial", "sans-serif"],
    "font.size": 10,
    "axes.titlesize": 12,
    "axes.titleweight": "bold",
    "axes.labelsize": 10,
    "figure.facecolor": "white",
    "axes.facecolor": PANEL,
    "axes.edgecolor": GRID,
    "axes.linewidth": 0.8,
    "axes.grid": True,
    "grid.alpha": 0.45,
    "grid.color": GRID,
    "grid.linestyle": "-",
    "grid.linewidth": 0.6,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "legend.framealpha": 0.92,
    "legend.edgecolor": GRID,
}


def _style_axes(ax: plt.Axes) -> None:
    ax.set_facecolor(PANEL)
    ax.tick_params(colors=INK, labelsize=9)
    ax.xaxis.label.set_color(INK)
    ax.yaxis.label.set_color(INK)
    ax.title.set_color(INK)


def _footer(fig: plt.Figure, text: str = "TPC-H · concurrency=1 · same hardware · docs/BENCHMARK.md") -> None:
    fig.text(0.5, 0.012, text, ha="center", fontsize=8, color=MUTED)


def _heatmap_cmap() -> mcolors.LinearSegmentedColormap:
    """Soft diverging map aligned with benchmark-summary: teal (DC wins) → slate → violet (SN wins)."""
    return mcolors.LinearSegmentedColormap.from_list(
        "dc_sn_soft",
        [
            "#0d9488",  # teal-600 — DC much faster
            "#5eead4",  # teal-300
            "#e0e7ff",  # indigo-100 — parity
            "#c4b5fd",  # violet-300
            "#7c3aed",  # violet-600 — SN faster
            "#4338ca",  # indigo-700 — SN much slower
        ],
        N=256,
    )


def _heatmap_text_color(log_val: float) -> str:
    """Readable labels without harsh white-on-bright."""
    if log_val <= -1.0:
        return "#f8fafc"
    if log_val >= 1.6:
        return "#f8fafc"
    return "#334155"


def plot_suite_convergence(ax: plt.Axes) -> None:
    x = np.arange(len(SCALE_LABELS))
    ax.fill_between(x, 0.25, 1.0, color=WIN, alpha=0.10, zorder=0)
    ax.axhline(1.0, color=WIN, linestyle=(0, (6, 4)), linewidth=1.8, alpha=0.85, zorder=1)
    ax.plot(x, SUITE_RATIOS, "o-", color=DC, linewidth=3, markersize=10,
            markerfacecolor="white", markeredgewidth=2.5, zorder=3)
    for i, (sf, ratio) in enumerate(zip(SCALE_LABELS, SUITE_RATIOS)):
        dy = -0.22 if ratio > 8 else 0.18
        ax.annotate(
            f"{ratio:.1f}×",
            (i, ratio),
            textcoords="offset points",
            xytext=(0, 14 if dy > 0 else -18),
            ha="center",
            fontsize=9,
            fontweight="bold",
            color=DC,
        )
    ax.set_yscale("log")
    ax.set_ylim(0.28, 100)
    ax.set_xticks(x)
    ax.set_xticklabels(SCALE_LABELS)
    ax.set_title("Suite overhead converges to parity")
    ax.set_ylabel("DuckCluster / single-node (p50 sum)")
    ax.text(0.02, 0.06, "Below dashed line → DC faster", transform=ax.transAxes,
            fontsize=8, color=WIN, fontweight="bold")
    _style_axes(ax)


def plot_dc_wins_stacked(ax: plt.Axes) -> None:
    x = np.arange(len(SCALE_LABELS))
    wins = np.array(DC_WINS, dtype=float)
    near = np.array(NEAR_PARITY, dtype=float)
    lose = 22 - wins - near

    ax.bar(x, wins, 0.62, color=WIN, label="DC faster", edgecolor="white", linewidth=0.8, zorder=2)
    ax.bar(x, near, 0.62, bottom=wins, color=DC_LIGHT, label="Near-parity (<1.15×)",
           edgecolor="white", linewidth=0.8, zorder=2)
    ax.bar(x, lose, 0.62, bottom=wins + near, color="#e5e7eb", label="SN faster",
           edgecolor="white", linewidth=0.8, zorder=2)

    for i, w in enumerate(DC_WINS):
        if w:
            ax.text(i, w / 2, str(int(w)), ha="center", va="center",
                    fontsize=11, fontweight="bold", color="white")

    ax.set_xticks(x)
    ax.set_xticklabels(SCALE_LABELS)
    ax.set_ylim(0, 24)
    ax.set_yticks([0, 5, 10, 15, 20, 22])
    ax.set_title("Per-query wins accelerate at scale")
    ax.set_ylabel("Queries (of 22)")
    ax.legend(loc="upper left", fontsize=8, ncol=1)
    _style_axes(ax)


def plot_crossover_lines(ax: plt.Axes) -> None:
    x = np.arange(len(SCALE_LABELS))
    palette = plt.cm.tab10(np.linspace(0, 1, len(CROSSOVER_QUERIES)))
    markers = ["o", "s", "^", "D", "v", "P", "X", "*", "h"]

    ax.fill_between(x, 0.2, 1.0, color=WIN, alpha=0.08, zorder=0)
    ax.axhline(1.0, color=MUTED, linewidth=1.5, zorder=1)

    for (query, ratios), color, marker in zip(CROSSOVER_QUERIES.items(), palette, markers):
        ax.plot(x, ratios, f"{marker}-", color=color, linewidth=2.2, markersize=6.5,
                label=query, markerfacecolor="white", markeredgewidth=1.5, zorder=3)

    ax.set_yscale("log")
    ax.set_ylim(0.28, 140)
    ax.set_xticks(x)
    ax.set_xticklabels(SCALE_LABELS)
    ax.set_title("Query crossover trajectories")
    ax.set_ylabel("DC / single-node ratio")
    ax.legend(ncol=3, fontsize=7.5, loc="upper right", columnspacing=0.8, handletextpad=0.4)
    _style_axes(ax)


def plot_sf40_dumbbell(ax: plt.Axes) -> None:
    ratios = [d / s for d, s in zip(SF40["dc_ms"], SF40["sn_ms"])]
    log_r = [math.log2(r) for r in ratios]
    y = np.arange(len(SF40["queries"]))

    for i, (lr, r) in enumerate(zip(log_r, ratios)):
        color = WIN if r < 1 else SN if r > 1.15 else DC
        ax.plot([0, lr], [i, i], color=color, linewidth=2.2, alpha=0.75, zorder=2)
        ax.scatter([lr], [i], s=52, color=color, edgecolors="white", linewidths=0.8, zorder=3)

    ax.axvline(0, color=INK, linewidth=1.2, alpha=0.35, zorder=1)
    ax.set_yticks(y)
    ax.set_yticklabels(SF40["queries"], fontsize=8.5, fontfamily="monospace")
    ax.set_xlabel("← DuckCluster faster          single-node faster →")
    ax.set_title("SF40 per-query ratio (log₂ scale)")
    ax.set_xlim(-2.4, 3.4)
    ax.set_xticks([-2, -1, 0, 1, 2, 3])
    ax.set_xticklabels(["4× faster", "2×", "parity", "2×", "4×", "8×"])
    ax.invert_yaxis()

    win_count = sum(1 for r in ratios if r < 1)
    ax.text(0.98, 0.02, f"{win_count}/22 DC wins", transform=ax.transAxes,
            ha="right", va="bottom", fontsize=9, fontweight="bold", color=WIN,
            bbox=dict(boxstyle="round,pad=0.35", facecolor="white", edgecolor=WIN, alpha=0.9))
    _style_axes(ax)


def plot_scaling_curves(ax: plt.Axes) -> None:
    x = np.arange(len(SCALE_LARGE))
    ax.plot(x, DC_SUITE_S, "o-", color=DC, linewidth=3, markersize=10, label="DuckCluster", zorder=3)
    ax.plot(x, SN_SUITE_S, "s-", color=SN, linewidth=3, markersize=10, label="Single-node", zorder=3)

    for i, (dc, sn) in enumerate(zip(DC_SUITE_S, SN_SUITE_S)):
        ratio = dc / sn
        ax.annotate(f"{ratio:.1f}×", (i, max(dc, sn) + 8), ha="center",
                    fontsize=9, fontweight="bold", color=MUTED)

    ax.set_xticks(x)
    ax.set_xticklabels(SCALE_LARGE)
    ax.set_ylabel("Suite time (seconds, p50 sum)")
    ax.set_title("DC scales sub-linearly; SN hits memory pressure")
    ax.legend(loc="upper left", fontsize=9)
    ax.text(0.98, 0.05, "DC 1.57× for 2× data\nSN 2.36× for 2× data",
            transform=ax.transAxes, ha="right", va="bottom", fontsize=8,
            color=MUTED, bbox=dict(boxstyle="round,pad=0.3", facecolor="white", edgecolor=GRID))
    _style_axes(ax)


def plot_crossover_heatmap(ax: plt.Axes) -> None:
    matrix = np.array([[CROSSOVER_QUERIES[q][i] for i in range(len(SCALE_LABELS))]
                       for q in HEATMAP_QUERIES])
    log_matrix = np.log2(matrix)

    norm = mcolors.TwoSlopeNorm(vmin=-2.5, vcenter=0, vmax=3.0)
    cmap = _heatmap_cmap()
    im = ax.imshow(log_matrix, aspect="auto", cmap=cmap, norm=norm, interpolation="nearest")

    # Light cell borders for readability
    ax.set_xticks(np.arange(len(SCALE_LABELS)) - 0.5, minor=True)
    ax.set_yticks(np.arange(len(HEATMAP_QUERIES)) - 0.5, minor=True)
    ax.grid(which="minor", color="white", linewidth=1.8, alpha=0.85)
    ax.tick_params(which="minor", bottom=False, left=False)

    ax.set_xticks(np.arange(len(SCALE_LABELS)))
    ax.set_xticklabels(SCALE_LABELS, fontsize=9)
    ax.set_yticks(np.arange(len(HEATMAP_QUERIES)))
    ax.set_yticklabels(HEATMAP_QUERIES, fontsize=9, fontfamily="monospace")
    ax.set_title("Crossover heatmap (log₂ DC/SN)")

    for i in range(len(HEATMAP_QUERIES)):
        for j in range(len(SCALE_LABELS)):
            val = matrix[i, j]
            text = f"{val:.1f}×" if val < 10 else f"{val:.0f}×"
            lum = log_matrix[i, j]
            ax.text(
                j, i, text,
                ha="center", va="center",
                fontsize=7.5, color=_heatmap_text_color(lum),
                fontweight="semibold",
            )

    cbar = plt.colorbar(im, ax=ax, fraction=0.028, pad=0.02)
    cbar.set_label("log₂(DC/SN)  ·  teal = DC faster  ·  violet = SN faster", fontsize=8)
    cbar.ax.tick_params(labelsize=7)
    cbar.outline.set_edgecolor(GRID)

    # Legend strip under title
    ax.text(
        0.5, 1.04,
        "← DuckCluster faster          parity          single-node faster →",
        transform=ax.transAxes, ha="center", fontsize=8, color=MUTED,
    )
    _style_axes(ax)


def plot_sf40_grouped_bars(ax: plt.Axes) -> None:
    """Side-by-side latency at SF40 for README hero."""
    top_n = 8
    winners = sorted(
        zip(SF40["queries"], SF40["dc_ms"], SF40["sn_ms"]),
        key=lambda t: t[1] / t[2],
    )[:top_n]

    queries = [w[0] for w in winners]
    dc = [w[1] / 1000 for w in winners]
    sn = [w[2] / 1000 for w in winners]

    y = np.arange(len(queries))
    h = 0.36
    ax.barh(y - h / 2, dc, h, color=DC, label="DuckCluster", edgecolor="white", linewidth=0.6)
    ax.barh(y + h / 2, sn, h, color=SN, label="Single-node", edgecolor="white", linewidth=0.6)

    for i, (d, s) in enumerate(zip(dc, sn)):
        ratio = d / s
        label = f"{ratio:.2f}×" if ratio >= 1 else f"{1/ratio:.2f}× faster"
        color = SN if ratio >= 1 else WIN
        ax.text(max(d, s) + 0.4, i, label, va="center", fontsize=8, fontweight="bold", color=color)

    ax.set_yticks(y)
    ax.set_yticklabels(queries, fontsize=9, fontfamily="monospace")
    ax.set_xlabel("Latency (seconds, p50)")
    ax.set_title("SF40 — strongest DC wins")
    ax.legend(loc="lower right", fontsize=8)
    ax.invert_yaxis()
    _style_axes(ax)


def save_benchmark_hero() -> Path:
    """Wide dashboard for README — primary visual."""
    plt.rcParams.update(STYLE)
    fig = plt.figure(figsize=(14, 5.2))
    gs = fig.add_gridspec(1, 3, width_ratios=[1.15, 1, 1.05], wspace=0.28)

    ax0 = fig.add_subplot(gs[0, 0])
    ax1 = fig.add_subplot(gs[0, 1])
    ax2 = fig.add_subplot(gs[0, 2])

    plot_suite_convergence(ax0)
    plot_dc_wins_stacked(ax1)
    plot_sf40_grouped_bars(ax2)

    fig.suptitle(
        "DuckCluster vs single-node DuckDB on TPC-H",
        fontsize=15,
        fontweight="bold",
        color=INK,
        y=0.98,
    )
    _footer(fig)
    fig.subplots_adjust(left=0.06, right=0.98, top=0.88, bottom=0.12, wspace=0.32)

    path = OUTPUT_DIR / "benchmark-hero.png"
    fig.savefig(path, dpi=180, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return path


def save_benchmark_summary() -> Path:
    """Four-panel composite (legacy README embed)."""
    plt.rcParams.update(STYLE)
    fig, axes = plt.subplots(2, 2, figsize=(12.5, 8.5))
    fig.suptitle("DuckCluster vs single-node DuckDB · TPC-H SF0.01–SF40",
                 fontsize=13, fontweight="bold", color=INK, y=0.98)

    plot_suite_convergence(axes[0, 0])
    plot_dc_wins_stacked(axes[0, 1])
    plot_crossover_lines(axes[1, 0])
    plot_sf40_dumbbell(axes[1, 1])

    _footer(fig)
    fig.tight_layout(rect=[0, 0.03, 1, 0.95])

    path = OUTPUT_DIR / "benchmark-summary.png"
    fig.savefig(path, dpi=170, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return path


def save_crossover_heatmap() -> Path:
    plt.rcParams.update(STYLE)
    fig, ax = plt.subplots(figsize=(9, 6.5))
    plot_crossover_heatmap(ax)
    _footer(fig)
    fig.tight_layout(rect=[0, 0.04, 1, 1])
    path = OUTPUT_DIR / "crossover-heatmap.png"
    fig.savefig(path, dpi=170, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return path


def save_scaling_curves() -> Path:
    plt.rcParams.update(STYLE)
    fig, ax = plt.subplots(figsize=(7.5, 4.5))
    plot_scaling_curves(ax)
    _footer(fig)
    fig.tight_layout(rect=[0, 0.05, 1, 1])
    path = OUTPUT_DIR / "scaling-curves.png"
    fig.savefig(path, dpi=170, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return path


def save_sf40_diverging() -> Path:
    plt.rcParams.update(STYLE)
    fig, ax = plt.subplots(figsize=(8.5, 10))
    plot_sf40_dumbbell(ax)
    _footer(fig)
    fig.tight_layout(rect=[0, 0.03, 1, 1])
    path = OUTPUT_DIR / "sf40-diverging.png"
    fig.savefig(path, dpi=170, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return path


def main() -> None:
    import sys

    only = set(sys.argv[1:]) if len(sys.argv) > 1 else set()
    generators = {
        "hero": save_benchmark_hero,
        "summary": save_benchmark_summary,
        "heatmap": save_crossover_heatmap,
        "scaling": save_scaling_curves,
        "sf40": save_sf40_diverging,
    }
    if only:
        targets = [generators[k] for k in generators if k in only]
    else:
        targets = list(generators.values())

    print("Generating benchmark plots...")
    paths = [fn() for fn in targets]
    for path in paths:
        print(f"  {path.relative_to(ROOT)}")
    print("Done.")


if __name__ == "__main__":
    main()

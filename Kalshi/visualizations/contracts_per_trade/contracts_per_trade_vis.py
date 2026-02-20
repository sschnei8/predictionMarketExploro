import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
import pandas as pd
import numpy as np
from datetime import datetime

# ── Data ──────────────────────────────────────────────────────────────────────
raw = pd.read_csv('contracts_per_trade_data.csv')

raw['MONTH'] = pd.to_datetime(raw['MONTH'])

dates  = raw['MONTH'].tolist()
cpt    = raw['contracts_per_trade'].tolist()
median = raw['median_contracts_per_trade'].tolist()
n      = len(dates)

# ── Colors ────────────────────────────────────────────────────────────────────
BG       = "#0a0a0f"
SURFACE  = "#111118"
BORDER   = "#1e1e2e"
TEXT     = "#e8e8f0"
MUTED    = "#5a5a78"

# deep plum → dusty rose (matches JS colorScale)
cmap = mcolors.LinearSegmentedColormap.from_list(
    "plum_rose", ["#3a2a4a", "#c97b8e"]
)
norm     = mcolors.Normalize(vmin=0, vmax=max(cpt))
bar_cols = [cmap(norm(v)) for v in cpt]

# ── Figure ────────────────────────────────────────────────────────────────────
fig, ax = plt.subplots(figsize=(11, 4.2), facecolor=BG)
ax.set_facecolor(SURFACE)

# Subtle grid
ax.yaxis.set_major_locator(plt.MaxNLocator(5))
ax.grid(axis="y", color=BORDER, linewidth=0.6, linestyle=(0, (2, 4)), zorder=0)
ax.set_axisbelow(True)

# Bars
x_pos = np.arange(n)
bars = ax.bar(x_pos, cpt, color=bar_cols, width=0.75, zorder=3, linewidth=0)

# Round tops via a thin cap line (visual only)
for xi, yi, col in zip(x_pos, cpt, bar_cols):
    ax.plot([xi - 0.375, xi + 0.375], [yi, yi],
            color=col, linewidth=1.2, solid_capstyle="round", zorder=4)

# ── Median line overlay ───────────────────────────────────────────────────────
MEDIAN_COLOR = "#c8a96e"  # warm gold — same as dashboard accent1
ax.plot(x_pos, median,
        color=MEDIAN_COLOR, linewidth=1.6, zorder=5,
        solid_capstyle="round", solid_joinstyle="round")
# Dot markers at each month
ax.scatter(x_pos, median,
           color=MEDIAN_COLOR, s=18, zorder=6, linewidths=0)
# Subtle glow halo behind the line
ax.plot(x_pos, median,
        color=MEDIAN_COLOR, linewidth=5, zorder=4, alpha=0.12,
        solid_capstyle="round")

# ── X-axis ticks ─────────────────────────────────────────────────────────────
tick_step = 8
tick_idx  = list(range(0, n, tick_step))
ax.set_xticks(tick_idx)
ax.set_xticklabels(
    [dates[i].strftime("'%y %m") for i in tick_idx],
    color=MUTED, fontsize=8.5, fontfamily="monospace"
)
ax.set_xlim(-0.8, n - 0.2)

# ── Y-axis ────────────────────────────────────────────────────────────────────
ax.set_ylim(0, max(cpt) * 1.08)
ax.yaxis.set_tick_params(labelcolor=MUTED, labelsize=8.5)
for lbl in ax.get_yticklabels():
    lbl.set_fontfamily("monospace")

# ── Spines ────────────────────────────────────────────────────────────────────
for spine in ax.spines.values():
    spine.set_visible(False)
ax.tick_params(axis="both", length=0)

# ── Gold top-edge accent line ─────────────────────────────────────────────────
fig.add_artist(
    plt.Line2D([0.02, 0.98], [1.0, 1.0],
               transform=fig.transFigure,
               color="#c8a96e", linewidth=0.8, alpha=0.5)
)

# ── Colorbar ─────────────────────────────────────────────────────────────────
# sm   = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
# cbar = fig.colorbar(sm, ax=ax, orientation="vertical",
#                     fraction=0.018, pad=0.015, shrink=0.85)
# cbar.ax.yaxis.set_tick_params(labelcolor=MUTED, labelsize=8)
# for lbl in cbar.ax.get_yticklabels():
#     lbl.set_fontfamily("monospace")
# cbar.outline.set_visible(False)
# cbar.ax.set_facecolor(SURFACE)

# ── Labels ────────────────────────────────────────────────────────────────────
ax.set_title(
    "CONTRACTS PER TRADE",
    color=MUTED, fontsize=9, fontfamily="monospace",
    loc="left", pad=12
)

# ── Legend ────────────────────────────────────────────────────────────────────
legend_elements = [
    Patch(facecolor="#7a4a6a", label="Avg contracts / trade"),
    Line2D([0], [0], color=MEDIAN_COLOR, linewidth=1.6,
           marker='o', markersize=4, label="Median contracts / trade"),
]
leg = ax.legend(
    handles=legend_elements,
    loc="upper left",
    frameon=False,
    fontsize=8,
    labelcolor=MUTED,
    handlelength=1.6,
    handletextpad=0.6,
    borderpad=0,
)
for text in leg.get_texts():
    text.set_fontfamily("monospace")
fig.text(0.985, 0.97, "Jun 2021 – Feb 2026",
         ha="right", va="top", color=MUTED, fontsize=8, fontfamily="monospace")

plt.tight_layout(rect=[0, 0, 1, 0.97])
plt.savefig("contracts_per_trade.png",
            dpi=1080, bbox_inches="tight", facecolor=BG)
print("saved")
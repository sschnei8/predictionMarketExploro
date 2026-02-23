import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import matplotlib.patches as mpatches
import matplotlib.patheffects as pe
import numpy as np
import pandas as pd
from matplotlib.collections import LineCollection

# ── Data ──────────────────────────────────────────────────────────────────────
# Expected columns: MONTH, TOTAL_REV_FROM_FEES
raw = pd.read_csv('total_fee_data.csv')
raw['MONTH'] = pd.to_datetime(raw['MONTH'])
raw = raw.sort_values('MONTH').reset_index(drop=True)

dates = raw['MONTH'].tolist()
rev   = raw['TOTAL_REV_FROM_FEES'].values.astype(float)
n     = len(dates)
x     = np.arange(n)

# ── Colors ────────────────────────────────────────────────────────────────────
BG          = "#0a0a0f"
SURFACE     = "#111118"
BORDER      = "#e8952f"
MUTED       = "#aaaabc"
LINE_COLOR  = "#e8955a"
DOT_COLOR   = "#f0b07a"
ANNOT_COLOR = "#c87040"

# Bar color ramp: dark coral at bottom → vivid orange at top (by revenue magnitude)
BAR_LOW  = "#3a1f0f"   # very dark burnt sienna
BAR_MID  = "#a04828"   # mid coral
BAR_HIGH = "#e8955a"   # full coral-orange (matches line accent)

# ── Figure ────────────────────────────────────────────────────────────────────
fig, ax = plt.subplots(figsize=(12, 5), facecolor=BG)
ax.set_facecolor(SURFACE)

ax.yaxis.set_major_locator(plt.MaxNLocator(6))
ax.grid(axis="y", color=BORDER, linewidth=0.6, linestyle=(0, (2, 5)), zorder=0)
ax.set_axisbelow(True)

# ── Bars (color-scaled by revenue magnitude) ──────────────────────────────────
bar_width = 0.85
norm      = plt.Normalize(vmin=rev.min(), vmax=rev.max())
cmap      = mcolors = __import__('matplotlib.colors', fromlist=['LinearSegmentedColormap']).LinearSegmentedColormap.from_list(
    "rev_ramp", [BAR_LOW, BAR_MID, BAR_HIGH]
)

bar_colors = [cmap(norm(v)) for v in rev]

bars = ax.bar(x, rev, width=bar_width, color=bar_colors,
              zorder=3, linewidth=0)

# Thin bright top-cap on each bar
for xi, yi, col in zip(x, rev, bar_colors):
    ax.plot([xi - bar_width/2, xi + bar_width/2], [yi, yi],
            color=DOT_COLOR, linewidth=0.9, alpha=0.7, zorder=4,
            solid_capstyle="round")

# ── Revenue labels inside / above each bar ────────────────────────────────────
y_ceil = rev.max() * 1.30
ax.set_ylim(0, y_ceil)

# Threshold: if bar is tall enough to fit text inside, put it inside; else above
MIN_INSIDE = y_ceil * 0.055   # ~5.5% of axis height

for xi, yi, col in zip(x, rev, bar_colors):
    if yi >= 1e6:
        lbl = f"${yi/1e6:.1f}M"
    elif yi >= 1e3:
        lbl = f"${yi/1e3:.0f}K"
    else:
        lbl = f"${yi:.0f}"

    if yi >= MIN_INSIDE:
        # Inside near the top of the bar
        ax.text(xi, yi - y_ceil * 0.015, lbl,
                ha="center", va="top",
                color=BG, fontsize=5.5, fontfamily="monospace",
                fontweight="bold", zorder=5)
    else:
        # Above the bar
        ax.text(xi, yi + y_ceil * 0.012, lbl,
                ha="center", va="bottom",
                color=DOT_COLOR, fontsize=5.5, fontfamily="monospace",
                zorder=5)

# ── MoM change labels (all above bars) ───────────────────────────────────────
for i in range(1, n):
    mom  = (rev[i] - rev[i-1]) / rev[i-1] * 100
    sign = "+" if mom >= 0 else ""
    col  = "#7bbf9a" if mom >= 0 else "#bf7b7b"
    # Place above the bar (or above the rev label if bar is tall)
    y_pos = rev[i] + y_ceil * 0.052
    ax.text(x[i], y_pos, f"{sign}{mom:.0f}%",
            ha="center", va="bottom",
            fontsize=5.8, fontfamily="monospace", color=col,
            zorder=6)

# ── X-axis ────────────────────────────────────────────────────────────────────
ax.set_xticks(x)
ax.set_xticklabels(
    [d.strftime("%b '%y") for d in dates],
    color=MUTED, fontsize=7.8, fontfamily="monospace", rotation=35, ha="right"
)
ax.set_xlim(-0.6, n - 0.4)

# ── Y-axis ────────────────────────────────────────────────────────────────────
ax.yaxis.set_tick_params(labelcolor=MUTED, labelsize=8.5)
ax.yaxis.set_major_formatter(mticker.FuncFormatter(
    lambda v, _: f"${v/1e6:.1f}M" if v >= 1e6
            else f"${v/1e3:.0f}K" if v >= 1e3
            else f"${v:.0f}"
))
for lbl in ax.get_yticklabels():
    lbl.set_fontfamily("monospace")

# ── Spines ────────────────────────────────────────────────────────────────────
for spine in ax.spines.values():
    spine.set_visible(False)
ax.tick_params(axis="both", length=0)

# ── Gold top-edge accent ──────────────────────────────────────────────────────
fig.add_artist(
    plt.Line2D([0.02, 0.98], [1.0, 1.0],
               transform=fig.transFigure,
               color="#c8a96e", linewidth=0.8, alpha=0.5)
)

# ── Total revenue callout (top right) ─────────────────────────────────────────
total_str = "$545.6M Total Rev"
fig.text(0.97, 0.93, total_str,
         ha="right", va="top", color=DOT_COLOR,
         fontsize=11, fontfamily="monospace", fontweight="bold")
fig.text(0.97, 0.89, f"{dates[0].strftime('%b %Y')} – {dates[-1].strftime('%b %Y')}",
         ha="right", va="top", color=MUTED, fontsize=7.5, fontfamily="monospace")

# ── Title ─────────────────────────────────────────────────────────────────────
ax.set_title(
    "TOTAL REVENUE FROM FEES BY MONTH",
    color=MUTED, fontsize=9, fontfamily="monospace",
    loc="left", pad=12
)

plt.tight_layout(rect=[0, 0, 1, 0.97])
plt.savefig("total_revenue_chart.png", dpi=300, bbox_inches="tight", facecolor=BG)
print("saved → total_revenue_chart.png")
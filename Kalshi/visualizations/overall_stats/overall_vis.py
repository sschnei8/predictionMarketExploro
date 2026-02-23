import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import matplotlib.gridspec as gridspec
import matplotlib.patches as FancyBboxPatch
import numpy as np
import pandas as pd
from matplotlib.colors import LinearSegmentedColormap

# ── Data ──────────────────────────────────────────────────────────────────────
stats  = pd.read_csv('overall_stats_data.csv')
weekly = pd.read_csv('weekly_overall_stats_data.csv')
weekly['WEEK'] = pd.to_datetime(weekly['WEEK'])
weekly = weekly.sort_values('WEEK').reset_index(drop=True)

dates  = weekly['WEEK'].tolist()
volume = weekly['TOTAL_VOLUME'].values.astype(float)
trades = weekly['TOTAL_TRADES'].values.astype(float)
n      = len(dates)
x      = np.arange(n)

# Pull summary stats (single row)
total_trades   = int(stats['TOTAL_TRADES'].iloc[0])
total_volume   = float(stats['TOTAL_VOLUME'].iloc[0])
avg_cpt        = float(stats['AVG_CONTRACTS_PER_TRADE'].iloc[0])
market_count   = int(stats['MARKET_COUNT'].iloc[0])

def fmt_large(v):
    if v >= 1e9:  return f"{v/1e9:.2f}B"
    if v >= 1e6:  return f"{v/1e6:.1f}M"
    if v >= 1e3:  return f"{v/1e3:.0f}K"
    return str(int(v))

STAT_BOXES = [
    ("TOTAL TRADES",          fmt_large(total_trades)),
    ("TOTAL VOLUME",          fmt_large(total_volume)),
    ("AVG CONTRACTS / TRADE", f"{avg_cpt:.1f}"),
    ("MARKETS",               fmt_large(market_count)),
]

# ── Colors ────────────────────────────────────────────────────────────────────
BG       = "#0a0a0f"
SURFACE  = "#111118"
BORDER   = "#1e1e2e"
MUTED    = "#aaaabc"

GREEN_DIM  = "#1a3a28"   # dark forest — dim weeks
GREEN_MID  = "#2d7a4f"   # mid emerald
GREEN_VIV  = "#4dbb78"   # vivid spring green
GREEN_DOT  = "#7ddfa0"   # highlight dots / labels
GREEN_GLOW = "#3daa65"

TRADE_LINE = "#8ecfa8"   # sage — secondary line

# ── Layout: stat boxes row + chart ────────────────────────────────────────────
fig = plt.figure(figsize=(14, 6.2), facecolor=BG)
gs  = gridspec.GridSpec(
    2, 1,
    height_ratios=[1, 3.8],
    hspace=0.28,
    figure=fig
)

# ── Top row: stat boxes ───────────────────────────────────────────────────────
ax_stats = fig.add_subplot(gs[0])
ax_stats.set_facecolor(BG)
ax_stats.axis("off")

n_boxes   = len(STAT_BOXES)
box_w     = 0.21
box_h     = 0.72
gap       = (1.0 - n_boxes * box_w) / (n_boxes + 1)

for i, (label, value) in enumerate(STAT_BOXES):
    bx = gap + i * (box_w + gap)
    by = 0.10

    # Box background
    rect = plt.Rectangle((bx, by), box_w, box_h,
                          transform=ax_stats.transAxes,
                          facecolor=SURFACE, edgecolor=GREEN_DIM,
                          linewidth=1.0, zorder=2,
                          clip_on=False)
    ax_stats.add_patch(rect)

    # Thin green top accent on each box
    ax_stats.plot([bx, bx + box_w], [by + box_h, by + box_h],
                  transform=ax_stats.transAxes,
                  color=GREEN_VIV, linewidth=1.2, alpha=0.7,
                  zorder=3, clip_on=False)

    # Value (large)
    ax_stats.text(bx + box_w / 2, by + box_h * 0.56, value,
                  transform=ax_stats.transAxes,
                  ha="center", va="center",
                  fontsize=17, fontfamily="monospace", fontweight="bold",
                  color=GREEN_DOT, zorder=3)

    # Label (small, below)
    ax_stats.text(bx + box_w / 2, by + box_h * 0.18, label,
                  transform=ax_stats.transAxes,
                  ha="center", va="center",
                  fontsize=7.2, fontfamily="monospace",
                  color=MUTED, zorder=3)

# ── Chart area ────────────────────────────────────────────────────────────────
ax = fig.add_subplot(gs[1])
ax.set_facecolor(SURFACE)

ax.yaxis.set_major_locator(plt.MaxNLocator(6))
ax.grid(axis="y", color=BORDER, linewidth=0.5, linestyle=(0, (2, 5)), zorder=0)
ax.set_axisbelow(True)

# Color-scale bars by volume magnitude
cmap_green = LinearSegmentedColormap.from_list(
    "greens", [GREEN_DIM, GREEN_MID, GREEN_VIV]
)
norm       = plt.Normalize(vmin=volume.min(), vmax=volume.max())
bar_colors = [cmap_green(norm(v)) for v in volume]

bar_width  = 0.75
ax.bar(x, volume, width=bar_width, color=bar_colors, zorder=3, linewidth=0)

# Bright top-cap per bar
for xi, yi, col in zip(x, volume, bar_colors):
    ax.plot([xi - bar_width/2, xi + bar_width/2], [yi, yi],
            color=GREEN_DOT, linewidth=0.7, alpha=0.5, zorder=4,
            solid_capstyle="round")

# ── Trade count line (right axis) ────────────────────────────────────────────
ax2 = ax.twinx()
ax2.set_facecolor("none")

# Glow layers
for lw, al in [(8, 0.04), (4, 0.08)]:
    ax2.plot(x, trades, color=TRADE_LINE, linewidth=lw, alpha=al, zorder=5,
             solid_capstyle="round")
ax2.plot(x, trades, color=TRADE_LINE, linewidth=1.4, zorder=6,
         solid_capstyle="round", linestyle=(0, (6, 2)))

ax2.set_ylim(0, trades.max() * 1.35)
ax2.yaxis.set_tick_params(labelcolor=MUTED, labelsize=8)
ax2.yaxis.set_major_formatter(mticker.FuncFormatter(
    lambda v, _: fmt_large(v)
))
for lbl in ax2.get_yticklabels():
    lbl.set_fontfamily("monospace")
for spine in ax2.spines.values():
    spine.set_visible(False)

# ── X-axis ticks (every ~8 weeks) ────────────────────────────────────────────
tick_step = max(1, n // 12)
tick_idx  = list(range(0, n, tick_step))
ax.set_xticks(tick_idx)
ax.set_xticklabels(
    [dates[i].strftime("%b '%y") for i in tick_idx],
    color=MUTED, fontsize=8, fontfamily="monospace", rotation=30, ha="right"
)
ax.set_xlim(-0.8, n - 0.2)

# ── Y-axis (left — volume) ────────────────────────────────────────────────────
y_ceil = volume.max() * 1.18
ax.set_ylim(0, y_ceil)
ax.yaxis.set_tick_params(labelcolor=MUTED, labelsize=8.5)
ax.yaxis.set_major_formatter(mticker.FuncFormatter(
    lambda v, _: fmt_large(v)
))
for lbl in ax.get_yticklabels():
    lbl.set_fontfamily("monospace")

for spine in ax.spines.values():
    spine.set_visible(False)
ax.tick_params(axis="both", length=0)
ax2.tick_params(axis="both", length=0)

# ── Legend ────────────────────────────────────────────────────────────────────
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
legend_elements = [
    Patch(facecolor=GREEN_MID, label="Weekly Volume"),
    Line2D([0], [0], color=TRADE_LINE, linewidth=1.4,
           linestyle=(0, (6, 2)), label="Weekly Trades"),
]
leg = ax.legend(
    handles=legend_elements,
    loc="upper left", frameon=False,
    fontsize=8, labelcolor=MUTED,
    handlelength=1.4, handletextpad=0.6,
    borderpad=0, labelspacing=0.5,
)
for t in leg.get_texts():
    t.set_fontfamily("monospace")

# ── Axis labels ───────────────────────────────────────────────────────────────
ax.set_ylabel("VOLUME", color=MUTED, fontsize=7.5,
              fontfamily="monospace", labelpad=8)
ax2.set_ylabel("TRADES", color=MUTED, fontsize=7.5,
               fontfamily="monospace", labelpad=8)

# ── Gold top-edge accent ──────────────────────────────────────────────────────
fig.add_artist(
    plt.Line2D([0.02, 0.98], [1.0, 1.0],
               transform=fig.transFigure,
               color="#c8a96e", linewidth=0.8, alpha=0.5)
)

# ── Title ─────────────────────────────────────────────────────────────────────
ax_stats.set_title(
    "MARKET OVERVIEW  ·  WEEKLY ACTIVITY",
    color=MUTED, fontsize=9, fontfamily="monospace",
    loc="left", pad=10, y=1.02
)

plt.savefig("weekly_overview_chart.png", dpi=300,
            bbox_inches="tight", facecolor=BG)
print("saved → weekly_overview_chart.png")
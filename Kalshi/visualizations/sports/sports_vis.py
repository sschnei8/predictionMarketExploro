import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import matplotlib.patches as mpatches
import numpy as np
import pandas as pd

# ── Data ──────────────────────────────────────────────────────────────────────
# Expected columns: Market_Type, Total_Volume, Volume_PCT
raw = pd.read_csv('sports_data.csv')
raw['Volume_PCT_float'] = raw['Volume_PCT'].str.rstrip('%').astype(float)
raw = raw.sort_values('Volume_PCT_float', ascending=True).reset_index(drop=True)

labels  = raw['Market_Type'].tolist()
volumes = raw['Total_Volume'].values.astype(float)
pcts    = raw['Volume_PCT_float'].values
n       = len(raw)
y       = np.arange(n)

# ── Colors ────────────────────────────────────────────────────────────────────
BG       = "#080808"
SURFACE  = "#101010"
BORDER   = "#1c1c1c"
MUTED    = "#aba6a6"
MUTED2   = "#b7b5b5"

# Monochrome ramp: near-black → bright white, scaled by pct share
FILL_LOW  = "#1e1e1e"
FILL_MID  = "#787878"
FILL_HIGH = "#e8e8e8"

# ── Figure ────────────────────────────────────────────────────────────────────
fig, ax = plt.subplots(figsize=(11, max(4, n * 0.62)), facecolor=BG)
ax.set_facecolor(SURFACE)

norm       = plt.Normalize(vmin=pcts.min(), vmax=pcts.max())
from matplotlib.colors import LinearSegmentedColormap
cmap_bw    = LinearSegmentedColormap.from_list("bw", [FILL_LOW, FILL_MID, FILL_HIGH])
bar_colors = [cmap_bw(norm(p)) for p in pcts]

bar_height = 0.58
hbars = ax.barh(y, volumes, height=bar_height,
                color=bar_colors, zorder=3, linewidth=0)

# Right-cap accent — brightness matches bar
for yi, xi, col in zip(y, volumes, bar_colors):
    ax.plot([xi, xi], [yi - bar_height/2, yi + bar_height/2],
            color=col, linewidth=1.2, alpha=0.8, zorder=4,
            solid_capstyle="round")

# ── Labels: volume + pct on each bar ─────────────────────────────────────────
max_vol    = volumes.max()
MIN_INSIDE = max_vol * 0.30   # bar must be > 30% of max to fit text inside

for yi, xi, pct, col in zip(y, volumes, pcts, bar_colors):
    # Choose text color: dark text on light bars, light text on dark bars
    brightness = col[0] * 0.299 + col[1] * 0.587 + col[2] * 0.114
    txt_color  = "#101010" if brightness > 0.45 else "#d0d0d0"

    vol_str = (f"{xi/1e9:.2f}B" if xi >= 1e9
          else f"{xi/1e6:.1f}M" if xi >= 1e6
          else f"{xi/1e3:.0f}K" if xi >= 1e3
          else str(int(xi)))
    full_lbl = f"{vol_str}  ·  {pct:.2f}%"

    if xi >= MIN_INSIDE:
        # Inside bar, right-aligned
        ax.text(xi - max_vol * 0.015, yi, full_lbl,
                va="center", ha="right",
                color=txt_color, fontsize=8, fontfamily="monospace",
                fontweight="bold", zorder=5)
    else:
        # Outside bar, just right of bar end
        ax.text(xi + max_vol * 0.018, yi, full_lbl,
                va="center", ha="left",
                color=MUTED2, fontsize=8, fontfamily="monospace",
                zorder=5)

# ── Y-axis: market type labels ────────────────────────────────────────────────
ax.set_yticks(y)
ax.set_yticklabels(labels, color=MUTED2, fontsize=9, fontfamily="monospace")
ax.set_ylim(-0.6, n - 0.4)

# ── X-axis ────────────────────────────────────────────────────────────────────
ax.set_xlim(0, max_vol * 1.28)
ax.xaxis.set_major_locator(plt.MaxNLocator(5))
ax.xaxis.set_major_formatter(mticker.FuncFormatter(
    lambda v, _: f"{v/1e9:.1f}B" if v >= 1e9
            else f"{v/1e6:.0f}M" if v >= 1e6
            else f"{v/1e3:.0f}K" if v >= 1e3
            else str(int(v))
))
ax.xaxis.set_tick_params(labelcolor=MUTED, labelsize=8)
for lbl in ax.get_xticklabels():
    lbl.set_fontfamily("monospace")

# Grid
ax.xaxis.grid(True, color=BORDER, linewidth=0.5,
              linestyle=(0, (2, 5)), zorder=0)
ax.set_axisbelow(True)

# ── Spines ────────────────────────────────────────────────────────────────────
for spine in ax.spines.values():
    spine.set_visible(False)
ax.tick_params(length=0)

# ── Colorbar (pct scale) ──────────────────────────────────────────────────────
sm   = plt.cm.ScalarMappable(cmap=cmap_bw, norm=norm)
cbar = fig.colorbar(sm, ax=ax, orientation="vertical",
                    fraction=0.016, pad=0.02, shrink=0.7)
cbar.ax.set_ylabel("% of total volume", color=MUTED, fontsize=7.5,
                   fontfamily="monospace", labelpad=8)
cbar.ax.yaxis.set_tick_params(labelcolor=MUTED, labelsize=7.5)
cbar.ax.yaxis.set_major_formatter(mticker.FuncFormatter(
    lambda v, _: f"{v:.1f}%"
))
for lbl in cbar.ax.get_yticklabels():
    lbl.set_fontfamily("monospace")
cbar.outline.set_visible(False)
cbar.ax.set_facecolor(SURFACE)

# ── Thin white top-edge accent ────────────────────────────────────────────────
fig.add_artist(
    plt.Line2D([0.02, 0.98], [1.0, 1.0],
               transform=fig.transFigure,
               color="#cccccc", linewidth=0.8, alpha=0.4)
)

# ── Title ─────────────────────────────────────────────────────────────────────
ax.set_title("VOLUME SHARE BY MARKET TYPE",
             color=MUTED2, fontsize=9, fontfamily="monospace",
             loc="left", pad=12)

plt.tight_layout(rect=[0, 0, 1, 0.97])
plt.savefig("market_type_volume_chart.png", dpi=300,
            bbox_inches="tight", facecolor=BG)
print("saved → market_type_volume_chart.png")
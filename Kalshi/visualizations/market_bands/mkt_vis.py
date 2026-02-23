import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import matplotlib.gridspec as gridspec
import numpy as np
import pandas as pd
from matplotlib.colors import LinearSegmentedColormap
from matplotlib.patches import Patch

# ── Data ──────────────────────────────────────────────────────────────────────
bands_raw  = pd.read_csv('mkt_band_data.csv')
yes_no_raw = pd.read_csv('yes_no_mkt_band_data.csv')

# Canonical band order (sort prefix ensures this)
BAND_ORDER = [
    'a. 0 volume',
    'b. <1000 volume',
    'c. 1000-10,000 volume',
    'd. 10,000-100,000 volume',
    'e. 100,000-1,000,000 volume',
    'f. 1,000,000-10,000,000 volume',
    'g. >= 10,000,000 volume',
]

# Clean labels for display (strip sort prefix)
def clean(b):
    return b.split('. ', 1)[-1] if '. ' in b else b

BAND_LABELS = [clean(b) for b in BAND_ORDER]

# Left chart: overall market count per band
bands_raw = bands_raw.set_index('VOLUME_BANDS').reindex(BAND_ORDER).fillna(0).reset_index()
mkt_counts = bands_raw['NUMBER_OF_MARKETS'].values.astype(float)

# Right chart: YES / NO ratio per band
yes_no_raw['result'] = yes_no_raw['result'].str.strip().str.lower()
pivot = (
    yes_no_raw.pivot_table(
        index='VOLUME_BANDS', columns='result',
        values='RATIO_PCT', aggfunc='sum', fill_value=0
    )
    .reindex(BAND_ORDER)
    .fillna(0)
)
yes_ratio = pivot.get('yes', pd.Series(0, index=BAND_ORDER)).values
no_ratio  = pivot.get('no',  pd.Series(0, index=BAND_ORDER)).values

# ── Colors ────────────────────────────────────────────────────────────────────
BG      = "#0a0a0f"
SURFACE = "#111118"
BORDER  = "#1e1e2e"
MUTED   = "#aaaabc"

# Left chart: teal-to-cyan ramp (volume bands)
TEAL_LOW  = "#1a3535"
TEAL_MID  = "#2a7070"
TEAL_HIGH = "#4db8b8"
TEAL_DOT  = "#7dd8d8"

# Right chart: YES = sage green, NO = muted rose
YES_COLOR = "#4dbb78"
YES_DIM   = "#1a3a28"
NO_COLOR  = "#c97b8e"
NO_DIM    = "#3a1a28"

n = len(BAND_ORDER)
y = np.arange(n)   # horizontal bars → y axis is bands

# ── Figure ────────────────────────────────────────────────────────────────────
fig = plt.figure(figsize=(15, 6), facecolor=BG)
gs  = gridspec.GridSpec(1, 2, figure=fig, wspace=0.38)

# ══════════════════════════════════════════════════════════════════════════════
# LEFT: Market count by volume band (horizontal bars)
# ══════════════════════════════════════════════════════════════════════════════
ax1 = fig.add_subplot(gs[0])
ax1.set_facecolor(SURFACE)

cmap_teal  = LinearSegmentedColormap.from_list("teal", [TEAL_MID, TEAL_HIGH])
norm_teal  = plt.Normalize(vmin=mkt_counts.min(), vmax=mkt_counts.max())
bar_colors = [cmap_teal(norm_teal(v)) for v in mkt_counts]

bar_height = 0.62
hbars = ax1.barh(y, mkt_counts, height=bar_height,
                 color=bar_colors, zorder=3, linewidth=0)

# Right-cap accent
for yi, xi, col in zip(y, mkt_counts, bar_colors):
    ax1.plot([xi, xi], [yi - bar_height/2, yi + bar_height/2],
             color=TEAL_DOT, linewidth=1.0, alpha=0.6, zorder=4,
             solid_capstyle="round")

# Value labels at end of each bar
for yi, xi in zip(y, mkt_counts):
    lbl = f"{int(xi):,}"
    ax1.text(xi + mkt_counts.max() * 0.015, yi, lbl,
             va="center", ha="left",
             color=TEAL_DOT, fontsize=8, fontfamily="monospace")

# Grid
ax1.xaxis.set_major_locator(plt.MaxNLocator(5))
ax1.grid(axis="x", color=BORDER, linewidth=0.5, linestyle=(0, (2, 5)), zorder=0)
ax1.set_axisbelow(True)

# Y-axis: band labels
ax1.set_yticks(y)
ax1.set_yticklabels(BAND_LABELS, color=MUTED, fontsize=8, fontfamily="monospace")
ax1.set_ylim(-0.6, n - 0.4)

# X-axis
ax1.xaxis.set_tick_params(labelcolor=MUTED, labelsize=8)
ax1.xaxis.set_major_formatter(mticker.FuncFormatter(
    lambda v, _: f"{v/1e3:.0f}K" if v >= 1e3 else str(int(v))
))
for lbl in ax1.get_xticklabels():
    lbl.set_fontfamily("monospace")

ax1.set_xlim(0, mkt_counts.max() * 1.18)

for spine in ax1.spines.values():
    spine.set_visible(False)
ax1.tick_params(length=0)

ax1.set_title("MARKETS BY VOLUME BAND",
              color=MUTED, fontsize=8.5, fontfamily="monospace",
              loc="left", pad=10)

# ══════════════════════════════════════════════════════════════════════════════
# RIGHT: YES / NO resolution ratio per band (stacked horizontal bar)
# ══════════════════════════════════════════════════════════════════════════════
ax2 = fig.add_subplot(gs[1])
ax2.set_facecolor(SURFACE)

# Stacked: YES first (left), NO second (right)
ax2.barh(y, yes_ratio, height=bar_height,
         color=YES_COLOR, zorder=3, linewidth=0, label="YES", alpha=0.85)
ax2.barh(y, no_ratio, height=bar_height, left=yes_ratio,
         color=NO_COLOR,  zorder=3, linewidth=0, label="NO",  alpha=0.85)

# Divider line between YES and NO
for yi, yr in zip(y, yes_ratio):
    if 0.02 < yr < 0.98:
        ax2.plot([yr, yr], [yi - bar_height/2, yi + bar_height/2],
                 color=BG, linewidth=1.5, zorder=5)

# Percentage labels inside each segment
for yi, yr, nr in zip(y, yes_ratio, no_ratio):
    if yr > 0.06:
        ax2.text(yr / 2, yi, f"{yr*100:.0f}%",
                 va="center", ha="center",
                 color=BG, fontsize=8, fontfamily="monospace",
                 fontweight="bold", zorder=6)
    if nr > 0.06:
        ax2.text(yr + nr / 2, yi, f"{nr*100:.0f}%",
                 va="center", ha="center",
                 color=BG, fontsize=8, fontfamily="monospace",
                 fontweight="bold", zorder=6)

# Grid
ax2.xaxis.set_major_locator(plt.MultipleLocator(0.25))
ax2.grid(axis="x", color=BORDER, linewidth=0.5, linestyle=(0, (2, 5)), zorder=0)
ax2.set_axisbelow(True)

# Axes
ax2.set_yticks(y)
ax2.set_yticklabels(BAND_LABELS, color=MUTED, fontsize=8, fontfamily="monospace")
ax2.set_xlim(0, 1.0)
ax2.set_ylim(-0.6, n - 0.4)
ax2.xaxis.set_tick_params(labelcolor=MUTED, labelsize=8)
ax2.xaxis.set_major_formatter(mticker.FuncFormatter(
    lambda v, _: f"{v*100:.0f}%"
))
for lbl in ax2.get_xticklabels():
    lbl.set_fontfamily("monospace")

for spine in ax2.spines.values():
    spine.set_visible(False)
ax2.tick_params(length=0)

ax2.set_title("% OF MARKETS YES / NO RESOLUTION RATIO BY BAND",
              color=MUTED, fontsize=8.5, fontfamily="monospace",
              loc="left", pad=10)

# Legend
leg = ax2.legend(
    handles=[Patch(facecolor=YES_COLOR, label="YES"),
             Patch(facecolor=NO_COLOR,  label="NO")],
    loc="upper left", bbox_to_anchor=(1.01, 1.0), frameon=False,
    fontsize=8, labelcolor=MUTED,
    handlelength=1.0, handletextpad=0.5,
    borderpad=0, labelspacing=0.4,
)
for t in leg.get_texts():
    t.set_fontfamily("monospace")

# ── Supertitle ────────────────────────────────────────────────────────────────
fig.text(0.02, 0.97, "VOLUME DISTRIBUTION BY MARKET",
         ha="left", va="top",
         color=MUTED, fontsize=9, fontfamily="monospace")

plt.savefig("volume_distribution_chart.png", dpi=300,
            bbox_inches="tight", facecolor=BG)
print("saved → volume_distribution_chart.png")